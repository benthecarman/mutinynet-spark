# End-to-end dependencies

The `operator_*.key` files in this directory are public regtest fixtures.
Never reuse these keys on a network that carries value.

The supplemental JavaScript suite uses these revisions:

- MutinyNet Spark fork: `06614d2e3535385f15aef3749b2c8a780f679ebc`
- ldk-server: `6d6d810714706c225ce7effc2163eff6a1b54221`

The Breez Lightning acceptance test was verified with these clean upstream
revisions:

- Spark Operators fork: `dad8bc6cf109b5946eec6860fb84bf247c8cae98`
- ldk-server: `6d6d810714706c225ce7effc2163eff6a1b54221`

The supplemental JavaScript suite builds the SDK and runs `e2e/e2e.sh`. Local
runs must set `SPARK_REF`, `SDK_REF`, and `LDK_SERVER_REF` if the checkouts are
not in a shared temporary checkout.

The Lightning acceptance test runs `e2e/ln-e2e.sh`. It uses the Spark checkout
only to build three local operators and uses the Rust Breez SDK at revision
`c7eecfe` as the wallet client. It does not patch the operator checkout.
The test also starts a pinned Electrs image and uses its local Esplora API for
Breez chain data. It tests BOLT11 in both directions and separate BOLT12 send
and receive flows. The runner builds the operators from a clean detached
worktree at `SPARK_REF`'s `HEAD`. Set `SPARK_OPERATOR_COMMIT` to test another
commit. Set `SPARK_REF` and `LDK_SERVER_REF` when the source checkouts are not
at their default paths.

Update a revision only with the tests and production dependency pin that need
the same behavior.
