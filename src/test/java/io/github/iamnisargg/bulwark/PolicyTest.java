package io.github.iamnisargg.bulwark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PolicyTest {

    private static Policy tracing(String name, List<String> trace) {
        return new Policy() {
            @Override
            public <T> T execute(CheckedSupplier<T> action) throws Exception {
                trace.add(name + ":before");
                try {
                    return action.get();
                } finally {
                    trace.add(name + ":after");
                }
            }
        };
    }

    @Test
    void chainRunsFirstPolicyOutermost() throws Exception {
        List<String> trace = new ArrayList<>();
        Policy chain = Policy.chain(tracing("outer", trace), tracing("middle", trace), tracing("inner", trace));

        String result = chain.execute(() -> {
            trace.add("action");
            return "done";
        });

        assertThat(result).isEqualTo("done");
        assertThat(trace).containsExactly(
                "outer:before", "middle:before", "inner:before",
                "action",
                "inner:after", "middle:after", "outer:after");
    }

    @Test
    void emptyChainIsIdentity() throws Exception {
        assertThat(Policy.chain().execute(() -> 42)).isEqualTo(42);
    }

    @Test
    void singlePolicyChainBehavesLikeThatPolicy() throws Exception {
        List<String> trace = new ArrayList<>();
        Policy.chain(tracing("only", trace)).execute(() -> "x");
        assertThat(trace).containsExactly("only:before", "only:after");
    }

    @Test
    void checkedExceptionsPropagateUnchangedFromExecute() {
        IOException failure = new IOException("boom");
        assertThatThrownBy(() -> Policy.identity().execute(() -> {
            throw failure;
        })).isSameAs(failure);
    }

    @Test
    void getRethrowsRuntimeExceptionsAsIs() {
        IllegalStateException failure = new IllegalStateException("boom");
        assertThatThrownBy(() -> Policy.identity().get(() -> {
            throw failure;
        })).isSameAs(failure);
    }

    @Test
    void getWrapsCheckedFailuresRaisedByThePolicy() {
        Policy failing = new Policy() {
            @Override
            public <T> T execute(CheckedSupplier<T> action) throws Exception {
                throw new IOException("policy failure");
            }
        };
        assertThatThrownBy(() -> failing.get(() -> "never"))
                .isInstanceOf(BulwarkException.class)
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void getRestoresInterruptFlagWhenInterrupted() {
        Policy interrupted = new Policy() {
            @Override
            public <T> T execute(CheckedSupplier<T> action) throws Exception {
                throw new InterruptedException("stop");
            }
        };
        try {
            assertThatThrownBy(() -> interrupted.get(() -> "never")).isInstanceOf(BulwarkException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void runExecutesVoidActions() {
        List<String> trace = new ArrayList<>();
        Policy.identity().run(() -> trace.add("ran"));
        assertThat(trace).containsExactly("ran");
    }
}
