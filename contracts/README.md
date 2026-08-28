# contracts/

Versioned interface artifacts shared between the platform (L1/L2/L3) and
the individual use cases (8.1, 8.2, 8.3) — not use-case-specific code, and
not platform implementation either. This is where the platform's promises
to its consumers get written down once they need to be written down at
all:

- the frozen OpenAPI export of the Semantic API
- Kafka topic schemas — media-stream and behavioral-event topics are
  separate topics with separate schemas, per the platform's Kafka design
- the shared recommendation response shape

This directory is empty right now, on purpose. With a single use case
(8.1) consuming the platform, the contract between them is implicit — it
lives in the Semantic API's own code and 8.1's own tests, and duplicating
it here would just be a second copy to keep in sync for no reader. It gets
populated when a second independent consumer (8.2) exists and the
contract between "what the platform promises" and "what a specific use
case assumes" stops being something one person can hold in their head.

Do not add anything here speculatively. If you're looking at this file
wondering what belongs in contracts/, the answer is: nothing yet.
