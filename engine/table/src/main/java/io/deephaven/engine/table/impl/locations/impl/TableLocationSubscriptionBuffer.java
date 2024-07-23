//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Intermediates between push-based subscription to a TableLocationProvider and polling on update source refresh.
 */
public class TableLocationSubscriptionBuffer implements TableLocationProvider.Listener {

    private static final Set<ImmutableTableLocationKey> EMPTY_TABLE_LOCATION_KEYS = Collections.emptySet();

    private final TableLocationProvider tableLocationProvider;

    private boolean subscribed = false;

    private final Object updateLock = new Object();

    // These sets represent adds and removes from completed transactions.
    private Set<ImmutableTableLocationKey> pendingLocationsAdded = EMPTY_TABLE_LOCATION_KEYS;
    private Set<ImmutableTableLocationKey> pendingLocationsRemoved = EMPTY_TABLE_LOCATION_KEYS;

    // These sets represent open transactions that are being accumulated.
    private final Set<Object> transactionTokens = new HashSet<>();
    private final Map<Object, Set<ImmutableTableLocationKey>> accumulatedLocationsAdded = new HashMap<>();
    private final Map<Object, Set<ImmutableTableLocationKey>> accumulatedLocationsRemoved = new HashMap<>();

    private TableDataException pendingException = null;

    public TableLocationSubscriptionBuffer(@NotNull final TableLocationProvider tableLocationProvider) {
        this.tableLocationProvider = Require.neqNull(tableLocationProvider, "tableLocationProvider");
    }

    public static final class LocationUpdate {
        private final Collection<ImmutableTableLocationKey> pendingAddedLocationKeys;
        private final Collection<ImmutableTableLocationKey> pendingRemovedLocations;

        public LocationUpdate(@NotNull final Collection<ImmutableTableLocationKey> pendingAddedLocationKeys,
                @NotNull final Collection<ImmutableTableLocationKey> pendingRemovedLocations) {
            this.pendingAddedLocationKeys = pendingAddedLocationKeys;
            this.pendingRemovedLocations = pendingRemovedLocations;
        }

        public Collection<ImmutableTableLocationKey> getPendingAddedLocationKeys() {
            return pendingAddedLocationKeys;
        }

        public Collection<ImmutableTableLocationKey> getPendingRemovedLocationKeys() {
            return pendingRemovedLocations;
        }
    }

    /**
     * Subscribe if needed, and return any pending location keys (or throw a pending exception) from the table location
     * provider. A given location key will only be returned by a single call to processPending() (unless state is
     * reset). No order is maintained internally. If a pending exception is thrown, this signals that the subscription
     * is no longer valid and no subsequent location keys will be returned.
     *
     * @return The collection of pending location keys.
     */
    public synchronized LocationUpdate processPending() {
        // TODO: Should I change this to instead re-use the collection?
        if (!subscribed) {
            if (tableLocationProvider.supportsSubscriptions()) {
                tableLocationProvider.subscribe(this);
            } else {
                // NB: Providers that don't support subscriptions don't tick - this single call to run is
                // sufficient.
                tableLocationProvider.refresh();
                tableLocationProvider.getTableLocationKeys()
                        .forEach(tlk -> handleTableLocationKeyAdded(tlk, tableLocationProvider));
            }
            subscribed = true;
        }
        final Collection<ImmutableTableLocationKey> resultLocationKeys;
        final Collection<ImmutableTableLocationKey> resultLocationsRemoved;
        final TableDataException resultException;
        synchronized (updateLock) {
            resultLocationKeys = pendingLocationsAdded;
            pendingLocationsAdded = EMPTY_TABLE_LOCATION_KEYS;
            resultLocationsRemoved = pendingLocationsRemoved;
            pendingLocationsRemoved = EMPTY_TABLE_LOCATION_KEYS;
            resultException = pendingException;
            pendingException = null;
        }

        if (resultException != null) {
            throw new TableDataException("Processed pending exception", resultException);
        }

        return new LocationUpdate(resultLocationKeys, resultLocationsRemoved);
    }

    /**
     * Unsubscribe and clear any state pending processing.
     */
    public synchronized void reset() {
        if (subscribed) {
            if (tableLocationProvider.supportsSubscriptions()) {
                tableLocationProvider.unsubscribe(this);
            }
            subscribed = false;
        }
        synchronized (updateLock) {
            pendingLocationsAdded = EMPTY_TABLE_LOCATION_KEYS;
            pendingLocationsRemoved = EMPTY_TABLE_LOCATION_KEYS;
            pendingException = null;
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // TableLocationProvider.Listener implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public void beginTransaction(final Object token) {
        synchronized (updateLock) {
            // Assert that we can start a new transaction with this token.
            Require.eqFalse(transactionTokens.contains(token), "transactionTokens.contains(token)");
            Require.eqFalse(accumulatedLocationsAdded.containsKey(token),
                    "accumulatedLocationsAdded.containsKey(token)");
            Require.eqFalse(accumulatedLocationsRemoved.containsKey(token),
                    "accumulatedLocationsRemoved.containsKey(token)");

            transactionTokens.add(token);
            accumulatedLocationsAdded.put(token, EMPTY_TABLE_LOCATION_KEYS);
            accumulatedLocationsRemoved.put(token, EMPTY_TABLE_LOCATION_KEYS);
        }
    }

    @Override
    public void endTransaction(final Object token) {
        synchronized (updateLock) {
            // Assert that this transaction is open.
            Require.eqTrue(transactionTokens.contains(token), "transactionTokens.contains(token)");

            final Set<ImmutableTableLocationKey> tokenLocationsAdded = accumulatedLocationsAdded.get(token);
            final Set<ImmutableTableLocationKey> tokenLocationsRemoved = accumulatedLocationsRemoved.get(token);

            if (pendingLocationsRemoved != EMPTY_TABLE_LOCATION_KEYS) {
                // Handle any locations that were pending as adds but removed by this transaction.
                for (final ImmutableTableLocationKey tableLocationKey : tokenLocationsRemoved) {
                    if (pendingLocationsAdded.remove(tableLocationKey)) {
                        continue;
                    }
                    pendingLocationsRemoved.add(tableLocationKey);
                }
            } else {
                pendingLocationsRemoved = tokenLocationsRemoved;
            }

            if (pendingLocationsAdded != EMPTY_TABLE_LOCATION_KEYS) {
                // Handle any locations that were pending as removes but added again by this transaction.
                for (final ImmutableTableLocationKey tableLocationKey : tokenLocationsAdded) {
                    if (pendingLocationsRemoved.remove(tableLocationKey)) {
                        continue;
                    }
                    pendingLocationsAdded.add(tableLocationKey);
                }
            } else {
                pendingLocationsAdded = tokenLocationsAdded;
            }

            // Clear all the storage for this transaction.
            transactionTokens.remove(token);
            accumulatedLocationsAdded.remove(token);
            accumulatedLocationsRemoved.remove(token);
        }
    }

    @Override
    public void handleTableLocationKeyAdded(
            @NotNull final ImmutableTableLocationKey tableLocationKey,
            final Object transactionToken) {
        synchronized (updateLock) {
            if (accumulatedLocationsAdded.get(transactionToken) == EMPTY_TABLE_LOCATION_KEYS) {
                accumulatedLocationsAdded.put(transactionToken, new HashSet<>());
            }
            final Set<ImmutableTableLocationKey> locationsAdded = accumulatedLocationsAdded.get(transactionToken);
            final Set<ImmutableTableLocationKey> locationsRemoved = accumulatedLocationsRemoved.get(transactionToken);

            // A single transaction should never add and remove the same location,
            Require.eqFalse(locationsRemoved.contains(tableLocationKey),
                    "locationsRemoved.contains(tableLocationKey)");

            locationsAdded.add(tableLocationKey);
        }
    }

    @Override
    public void handleTableLocationKeyRemoved(
            @NotNull final ImmutableTableLocationKey tableLocationKey,
            final Object transactionToken) {
        synchronized (updateLock) {
            if (accumulatedLocationsRemoved.get(transactionToken) == EMPTY_TABLE_LOCATION_KEYS) {
                accumulatedLocationsRemoved.put(transactionToken, new HashSet<>());
            }
            final Set<ImmutableTableLocationKey> locationsAdded = accumulatedLocationsAdded.get(transactionToken);
            final Set<ImmutableTableLocationKey> locationsRemoved = accumulatedLocationsRemoved.get(transactionToken);

            // A single transaction should never add and remove the same location,
            Require.eqFalse(locationsAdded.contains(tableLocationKey),
                    "locationsAdded.contains(tableLocationKey)");

            locationsRemoved.add(tableLocationKey);
        }
    }

    @Override
    public void handleException(@NotNull final TableDataException exception) {
        synchronized (updateLock) {
            pendingException = exception;
        }
    }
}
