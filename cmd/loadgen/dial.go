package main

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// roundRobinServiceConfig selects gRPC's round_robin load-balancing
// policy: an independent choice among every address the resolver
// currently reports, made per RPC -- not one TCP/HTTP2 connection pinned
// to whichever backend it first reached.
//
// That distinction is the whole fix this file exists for. A Kubernetes
// ClusterIP Service (deploy/kubernetes/worker-service.yaml) load-balances
// at the connection level: kube-proxy picks one backend pod when a TCP
// connection is opened, and every RPC multiplexed over that one long-lived
// HTTP/2 connection lands on that same pod for the connection's whole
// lifetime. A gRPC client that dials a ClusterIP once and reuses the
// connection -- which is exactly what a long-lived client does, and what
// this tool did before this file existed -- therefore talks to exactly
// one pod no matter how many replicas exist behind the Service, and
// nothing about that setup rebalances traffic as pods come and go.
//
// round_robin fixes this on the CLIENT side instead: gRPC's resolver
// layer reports every address a name currently maps to, the round_robin
// balancer keeps one subchannel (one HTTP/2 connection) open to each of
// them, and each new RPC is assigned to the next subchannel in turn. So
// the balancing this tool relies on happens here, in the gRPC client, not
// in Kubernetes' Service networking -- which is why the target actually
// matters (see dialTarget below): a ClusterIP Service still resolves to
// exactly one IP (its own virtual IP), so round_robin over it changes
// nothing. A HEADLESS Service (deploy/kubernetes/worker-service-headless.yaml,
// clusterIP: None) is what makes DNS return every ready pod's own IP, which
// is the address list round_robin actually needs to have anything to
// balance across.
const roundRobinServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`

// dialTarget connects with client-side, per-RPC round_robin balancing.
//
//   - A single address is dialed as given, so a "dns:///" target (the
//     production case: a headless Service's DNS name) uses grpc's built-in
//     DNS resolver, registered automatically just by importing
//     google.golang.org/grpc -- no separate resolver import needed. That
//     resolver periodically re-resolves the name, so pods being added,
//     replaced, or removed is reflected without restarting this process. A
//     plain "host:port" with no scheme is also accepted (grpc's
//     passthrough resolver treats it as exactly one address); round_robin
//     over one address is a harmless no-op.
//   - More than one address (a comma-separated -addr) is treated as a
//     fixed, static set via grpc's manual resolver
//     (google.golang.org/grpc/resolver/manual), installed just for this
//     one ClientConn through grpc.WithResolvers rather than registered
//     globally. This is for local testing against literal process
//     addresses with no DNS name unifying them (see
//     integration/workerservice), where there is no headless Service to
//     point a single "dns:///" target at.
//
// Either way, every RPC on the returned connection can land on a
// different address; cmd/worker's response-header instance identifier
// (internal/workerdiag, cmd/worker/instance.go) is what lets a caller
// actually confirm that happened instead of assuming it.
func dialTarget(ctx context.Context, addresses []string) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(roundRobinServiceConfig),
		grpc.WithBlock(),
		// otelgrpc's client stats handler starts the trace for every
		// request this tool makes -- loadgen is the first hop, so this is
		// where each distributed trace (loadgen -> worker -> coordinator)
		// actually begins -- and injects that trace context into the RPC,
		// for the worker's own server-side span (cmd/worker's
		// otelgrpc.NewServerHandler) to continue as a child. Unconditional
		// and safe with no telemetry backend configured; see
		// internal/telemetry's package doc.
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}

	if len(addresses) == 1 {
		conn, err := grpc.DialContext(dialCtx, addresses[0], opts...)
		if err != nil {
			return nil, fmt.Errorf("dialing %s: %w", addresses[0], err)
		}
		return conn, nil
	}

	builder := manual.NewBuilderWithScheme("loadgen-static")
	resolverAddrs := make([]resolver.Address, len(addresses))
	for i, addr := range addresses {
		resolverAddrs[i] = resolver.Address{Addr: addr}
	}
	builder.InitialState(resolver.State{Addresses: resolverAddrs})

	opts = append(opts, grpc.WithResolvers(builder))
	conn, err := grpc.DialContext(dialCtx, builder.Scheme()+":///loadgen", opts...)
	if err != nil {
		return nil, fmt.Errorf("dialing %v: %w", addresses, err)
	}
	return conn, nil
}
