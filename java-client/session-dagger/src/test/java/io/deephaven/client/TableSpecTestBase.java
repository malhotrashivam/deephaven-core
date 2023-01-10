package io.deephaven.client;

import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.table.TableSpec;
import org.junit.Test;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class TableSpecTestBase extends DeephavenSessionTestBase {
    private final TableSpec table;

    public TableSpecTestBase(TableSpec table) {
        this.table = Objects.requireNonNull(table);
    }

    @Test
    public void batch() throws TableHandleException, InterruptedException {
        try (final TableHandle handle = session.batch().execute(table)) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    @Test
    public void serial() throws TableHandleException, InterruptedException {
        try (final TableHandle handle = session.serial().execute(table)) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }
}