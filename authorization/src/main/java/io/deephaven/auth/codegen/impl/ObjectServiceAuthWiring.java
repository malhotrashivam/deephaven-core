//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.auth.codegen.impl;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.ServiceAuthWiring;
import io.deephaven.proto.backplane.grpc.FetchObjectRequest;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc;
import io.deephaven.proto.backplane.grpc.StreamRequest;
import io.grpc.ServerServiceDefinition;

/**
 * This interface provides type-safe authorization hooks for ObjectServiceGrpc.
 */
public interface ObjectServiceAuthWiring extends ServiceAuthWiring<ObjectServiceGrpc.ObjectServiceImplBase> {
    /**
     * Wrap the real implementation with authorization checks.
     *
     * @param delegate the real service implementation
     * @return the wrapped service implementation
     */
    default ServerServiceDefinition intercept(ObjectServiceGrpc.ObjectServiceImplBase delegate) {
        final ServerServiceDefinition service = delegate.bindService();
        final ServerServiceDefinition.Builder serviceBuilder =
                ServerServiceDefinition.builder(service.getServiceDescriptor());

        serviceBuilder.addMethod(ServiceAuthWiring.intercept(
                service, "FetchObject", null, this::onMessageReceivedFetchObject));
        serviceBuilder.addMethod(ServiceAuthWiring.intercept(
                service, "MessageStream", this::onCallStartedMessageStream, this::onMessageReceivedMessageStream));
        serviceBuilder.addMethod(ServiceAuthWiring.intercept(
                service, "OpenMessageStream", null, this::onMessageReceivedOpenMessageStream));
        serviceBuilder.addMethod(ServiceAuthWiring.intercept(
                service, "NextMessageStream", null, this::onMessageReceivedNextMessageStream));

        return serviceBuilder.build();
    }

    /**
     * Authorize a request to FetchObject.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke FetchObject
     */
    void onMessageReceivedFetchObject(AuthContext authContext, FetchObjectRequest request);

    /**
     * Authorize a request to open a client-streaming rpc MessageStream.
     *
     * @param authContext the authentication context of the request
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke MessageStream
     */
    void onCallStartedMessageStream(AuthContext authContext);

    /**
     * Authorize a request to MessageStream.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke MessageStream
     */
    void onMessageReceivedMessageStream(AuthContext authContext, StreamRequest request);

    /**
     * Authorize a request to OpenMessageStream.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke OpenMessageStream
     */
    void onMessageReceivedOpenMessageStream(AuthContext authContext, StreamRequest request);

    /**
     * Authorize a request to NextMessageStream.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke NextMessageStream
     */
    void onMessageReceivedNextMessageStream(AuthContext authContext, StreamRequest request);

    class AllowAll implements ObjectServiceAuthWiring {
        public void onMessageReceivedFetchObject(AuthContext authContext, FetchObjectRequest request) {}

        public void onCallStartedMessageStream(AuthContext authContext) {}

        public void onMessageReceivedMessageStream(AuthContext authContext, StreamRequest request) {}

        public void onMessageReceivedOpenMessageStream(AuthContext authContext, StreamRequest request) {}

        public void onMessageReceivedNextMessageStream(AuthContext authContext, StreamRequest request) {}
    }

    class DenyAll implements ObjectServiceAuthWiring {
        public void onMessageReceivedFetchObject(AuthContext authContext, FetchObjectRequest request) {
            ServiceAuthWiring.operationNotAllowed();
        }

        public void onCallStartedMessageStream(AuthContext authContext) {
            ServiceAuthWiring.operationNotAllowed();
        }

        public void onMessageReceivedMessageStream(AuthContext authContext, StreamRequest request) {
            ServiceAuthWiring.operationNotAllowed();
        }

        public void onMessageReceivedOpenMessageStream(AuthContext authContext, StreamRequest request) {
            ServiceAuthWiring.operationNotAllowed();
        }

        public void onMessageReceivedNextMessageStream(AuthContext authContext, StreamRequest request) {
            ServiceAuthWiring.operationNotAllowed();
        }
    }

    class TestUseOnly implements ObjectServiceAuthWiring {
        public ObjectServiceAuthWiring delegate;

        public void onMessageReceivedFetchObject(AuthContext authContext, FetchObjectRequest request) {
            if (delegate != null) {
                delegate.onMessageReceivedFetchObject(authContext, request);
            }
        }

        public void onCallStartedMessageStream(AuthContext authContext) {
            if (delegate != null) {
                delegate.onCallStartedMessageStream(authContext);
            }
        }

        public void onMessageReceivedMessageStream(AuthContext authContext, StreamRequest request) {
            if (delegate != null) {
                delegate.onMessageReceivedMessageStream(authContext, request);
            }
        }

        public void onMessageReceivedOpenMessageStream(AuthContext authContext, StreamRequest request) {
            if (delegate != null) {
                delegate.onMessageReceivedOpenMessageStream(authContext, request);
            }
        }

        public void onMessageReceivedNextMessageStream(AuthContext authContext, StreamRequest request) {
            if (delegate != null) {
                delegate.onMessageReceivedNextMessageStream(authContext, request);
            }
        }
    }
}
