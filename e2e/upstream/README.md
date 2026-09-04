# End-to-end dependencies

The `operator_*.key` files in this directory are public regtest fixtures.
Never reuse these keys on a network that carries value.

The full end-to-end suite uses these revisions:

- MutinyNet Spark fork: `06614d2e3535385f15aef3749b2c8a780f679ebc`
- ldk-server: `6d6d810714706c225ce7effc2163eff6a1b54221`

The Docker workflow checks out these exact revisions, builds the JavaScript
SDK, and runs `e2e/e2e.sh`. Local runs must build the SDK from the same Spark
checkout and set `SPARK_REF`, `SDK_REF`, and `LDK_SERVER_REF` if the checkouts
are not in `/tmp/opencode`.

Update a revision only with the tests and production dependency pin that need
the same behavior.
