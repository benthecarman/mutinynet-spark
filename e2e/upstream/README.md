# Test keys only

The `operator_*.key` files in this directory are public regtest fixtures.
Never reuse these keys on a network that carries value.

The full e2e suite uses these pinned revisions:

- MutinyNet Spark fork: `06614d2e3535385f15aef3749b2c8a780f679ebc`
- ldk-server: `6d6d810714706c225ce7effc2163eff6a1b54221`

The CI workflow checks out these exact revisions. Update the revisions and
the compatibility tests together.
