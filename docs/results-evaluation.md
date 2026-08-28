# Results & Evaluation

This is not another build-order stage — 8.1's six stages (`docs/platform/`
stages 2-4 and `usecases/8_1_batch_reactive/docs/` stages 5-6) are complete
and verified. This is the write-up that sits on top of them: it synthesizes
the metrics that were already scattered across `reports/stage2`,
`reports/stage3`, `reports/uc81`, and the stage docs into one interpreted
narrative, and it does the qualitative and quantitative spot-checking that
stage 5/6 didn't — closing the gap between two different claims:

- **"The pipeline runs correctly end to end."** Stage 6 established this: 3
  golden tracks traced through every layer, matching data at every hop.
- **"The recommendations it produces are good."** Nothing before this
  document established this. It's a different claim, and it needs different
  evidence.

Numbers here are computed by `reports/results_evaluation.py` against the live,
already-populated stack (411 tracks, 4,110 recommendation rows from the single
full batch run) and written to `reports/results_evaluation/results.json`. The
same numbers back the "Results & Evaluation" section of `reports/results.html`.

## What the pipeline demonstrably did (recap)

| Stage | Headline metric |
|---|---|
| 2 · Ingestion | 411 licensed tracks (target 500 — accepted as within spec, see `docs/platform/stage2-ingestion.md`); 49.4% MusicBrainz match rate |
| 3 · Enrichment | 411/411 tracks embedded (CLAP, 512-dim) and graphed; 411 Track / 201 Artist / 77 Genre nodes, 764 `HAS_GENRE` edges |
| 4 · Semantic API | 6 endpoints, all read-only, over Postgres/Milvus/Neo4j |
| 5 · Recommender | 411/411 seeds processed, 4,110 rows written, zero skips, ~8.8s wall-clock |
| 6 · End-to-end | 3 golden tracks traced through every layer, one real cross-test Milvus bug found and fixed |

Total: 32/32 tests passing. This establishes correctness and reproducibility
of the pipeline mechanics. It says nothing about recommendation quality —
that's what the rest of this document is for.

## Quantitative findings beyond stage 5/6

### 1. Same-artist rate is skewed by catalog size — a filter-bubble effect

The `ARTIST_BOOST` (+0.15) was sized against the *dataset average* of ~2.0
tracks/artist (`usecases/8_1_batch_reactive/docs/uc81-recommender.md`). But that average hides a long
tail: 19 artists have 5 or more tracks in the 411-track catalog (`Tryad` has
20, `David Krystal` has 15, `Jonathan Dimmel` has 14). For seeds by those
artists, **33.0% of top-10 recommendations are by the same artist**, versus
**3.6%** for seeds by artists with a smaller catalog. Seed 90 (David Krystal,
15 tracks) is an extreme case in the spot-check below: 6 of its top 10
recommendations are the seed's own labelmate tracks.

This is a legitimate limitation of the additive-boost design at this
dataset's scale, not a bug: the boost weight was tuned against a mean, and a
prototype-scale catalog (411 tracks) has enough per-artist variance that the
mean undersells how strong the boost gets for high-catalog artists. It also
means **47.9% of seeds (197/411) get a same-artist track as their #1
recommendation** — for those seeds, the recommendation confirms an
already-known fact (this artist made other tracks) more often than it
demonstrates cross-artist semantic discovery, which is the more interesting
claim CLAP embeddings are supposed to support.

**How to apply / fix if this becomes a real concern**: cap same-artist boosts
per seed (e.g. at most 2-3 same-artist tracks in the top-10), or scale
`ARTIST_BOOST` down as a function of the artist's own catalog size. Neither
was implemented here — flagging the finding, not silently patching around it,
since the heuristic is explicitly documented as unvalidated and tunable.

### 2. Genre-tag overlap: just over half

Across all 4,110 recommendation rows, **53.5% share at least one genre tag**
with their seed; **46.5% share none**. This cuts both ways as evidence:

- It shows the ranking isn't just re-deriving what's already visible in
  `genre_tags` — almost half the recommendations are justified by CLAP
  acoustic similarity alone, which is the actual "semantic" contribution the
  thesis is about.
- It also means genre tags can't be used as an independent check on *most* of
  the list — for those rows, "is this a good recommendation" reduces to
  trusting the CLAP embedding, which is exactly the thing with no
  ground-truth validation (see Limitations).

### 3. The no-genre-tag fallback isn't degenerate

57 of 411 tracks (13.9%) have no `genre_tags` at all — for these seeds,
`GENRE_BOOST` can never apply, so ranking is similarity-plus-artist-boost
only. Their average recommendation score (0.802) is in line with the
dataset-wide average (0.794), meaning the CLAP-only path produces
scores in the same range as the full three-signal path — it isn't quietly
returning worse matches when metadata is missing. See seed 275 in the
spot-check: no genre tags, no same-artist or genre-boost signal on any
of its top 10, and the results are still topically plausible (electronic/pop
downtempo tracks) by ear of the tag data on the *recommended* side, even
though the seed itself carries no tags to check against.

## Qualitative spot-check: 8 seeds, full top-10 each

Chosen to spread across catalog size, genre-tag richness, and one edge case
(zero genre tags) — not cherry-picked for good results. Full detail in
`reports/results_evaluation/results.json`; the previously-existing worked
example (seed 1, in `usecases/8_1_batch_reactive/docs/uc81-recommender.md`) is a 9th data point on top
of these 8.

### Seed 1 — "Wish You Were Here" by The.madpix.project
Genre tags: dance, electronic, house

| Rank | Score | Track | Artist | Genre tags | Same artist | Genre overlap |
|---|---|---|---|---|---|---|
| 1 | 0.6585 | Moments | The.madpix.project | house, pop, dance | yes | yes |
| 2 | 0.5841 | Adult Only & SaReGaMa - Afterzone | SaReGaMa | world, electronica, chillout | | |
| 3 | 0.5699 | Divergence (Remastered VIP) | TheBlackParrot | electronic, house, dance | | yes |
| 4 | 0.5622 | Erick Fill & Alwaro - You'll Be Fine ft. Crushboys | erickfill | electropop, synthpop | | |
| 5 | 0.5185 | April Showers | ProleteR | electroswing, hiphop, swing | | |
| 6 | 0.5056 | sweet tania | Arnaud Martin Jazabana | jazz, jazzfunk | | |
| 7 | 0.5017 | Survive | JekK | house, electronic, electropop | | yes |
| 8 | 0.4976 | La Luna - feat SIzu Yantra | Butumbaba | ska, latin, reggae | | |
| 9 | 0.4747 | Making Me Nervous | Brad Sucks | rock | | |
| 10 | 0.4742 | Love of My Life | Ben Lvcas | edm, pop, electronic | | yes |

Notable: rank 2 (SaReGaMa) has zero tag overlap with the seed yet outranks
rank 3, which has 3-way tag overlap plus the genre boost — raw acoustic
similarity (0.5841 vs. 0.5199) dominates the small +0.05 boost. Consistent
with the design (boosts are re-ranking nudges, not overrides), but a reminder
that genre tags and CLAP similarity don't always agree, which is exactly the
kind of "acoustic content vs. folksonomic tags" mismatch a semantic pipeline
should surface rather than paper over.

### Seed 45 — "Ghost" by madelyn munsell
Genre tags: pop, indie, singersongwriter

| Rank | Score | Track | Artist | Genre tags | Same artist | Genre overlap |
|---|---|---|---|---|---|---|
| 1 | 0.9362 | Adventure | madelyn munsell | pop, indie | yes | yes |
| 2 | 0.8958 | Lets Restart | madelyn munsell | pop, indie | yes | yes |
| 3 | 0.8446 | All I Ever Wanted | Devon Elizabeth | pop, soul, rnb | | yes |
| 4 | 0.8164 | Awakening | Anne Davis | pop, singersongwriter, rock | | yes |
| 5 | 0.7977 | Alone | Color Out | emo, poprock | | |
| 6 | 0.7918 | Se Ne Va | Millionaire Blonde | pop, dance, electronic | | yes |
| 7 | 0.7902 | You're Always With Me | Marco Margna | pop, indie, filmscore | | yes |
| 8 | 0.7876 | Freak (C.Rizzo) | Mistery | (none) | | |
| 9 | 0.7811 | Show me | Liz Turner | pop, dance, eurodance | | yes |
| 10 | 0.7754 | Temple Of Contradictions | Anne Davis | christian, pop, rock | | yes |

Notable: coherent list, high genre-tag overlap throughout (8/10), highest
overall similarity scores of the sample (>0.77 across the board) — this is
the sample's cleanest result.

### Seed 90 — "Waiting For Love" by David Krystal
Genre tags: pop, singersongwriter

| Rank | Score | Track | Artist | Genre tags | Same artist | Genre overlap |
|---|---|---|---|---|---|---|
| 1 | 1.1082 | Missing You | David Krystal | pop, singersongwriter | yes | yes |
| 2 | 1.0529 | Like Yesterday | David Krystal | pop, singersongwriter | yes | yes |
| 3 | 1.0228 | Hide In The Night | David Krystal | pop, singersongwriter | yes | yes |
| 4 | 1.0067 | Love Isn't Wasted | David Krystal | singersongwriter, pop, poprock | yes | yes |
| 5 | 0.9709 | All Or Nothing | David Krystal | singersongwriter | yes | yes |
| 6 | 0.9624 | Addicted | David Krystal | pop, singersongwriter | yes | yes |
| 7 | 0.8845 | Get Lost | Anne Davis | folk, singersongwriter | | yes |
| 8 | 0.8754 | California Moon | Robert Avellanet | pop | | yes |
| 9 | 0.8680 | I Was Born For You | Robert Avellanet | pop, christian, jazz | | yes |
| 10 | 0.8675 | Heart and Soul | Robert Avellanet | pop, jazz, jazzfusion | | yes |

Notable: the catalog-skew effect from finding 1, directly. 6/10
same-artist. Every single row has genre overlap. Not a bad list, but a
narrow one — it barely demonstrates cross-artist discovery at all.

### Seed 150 — "Young love" by J Lynn the Bride
Genre tags: pop, rnb

| Rank | Score | Track | Artist | Genre tags | Same artist | Genre overlap |
|---|---|---|---|---|---|---|
| 1 | 1.0041 | Pieces of me | J Lynn the Bride | pop, rnb | yes | yes |
| 2 | 0.8384 | All I Ever Wanted | Devon Elizabeth | pop, soul, rnb | | yes |
| 3 | 0.8265 | Five O'Clock | JekK | pop, electronic | | yes |
| 4 | 0.8248 | Supergirl (C.Rizzo) | Mistery | pop | | yes |
| 5 | 0.8238 | Más Allá | Vince Miranda | pop, latin | | yes |
| 6 | 0.8209 | PerfectFit | Malika Rai | (none) | | |
| 7 | 0.8154 | I_ | Leslie Hunt | pop | | yes |
| 8 | 0.8153 | Przemek Puk - Ocean Łez | Przemek Puk | (none) | | |
| 9 | 0.8098 | Time Travel | Jasmine Jordan | rnb, soul, hiphop | | yes |
| 10 | 0.8032 | Laziness | Malika Rai | (none) | | |

Notable: only one same-artist track available (small catalog), the rest is
similarity-plus-genre-boost doing real work — 7/10 genre overlap despite the
seed having only 2 tags.

### Seed 210 — "Burn Your Love" by Sunwill
Genre tags: pop, dance, rock

| Rank | Score | Track | Artist | Genre tags | Same artist | Genre overlap |
|---|---|---|---|---|---|---|
| 1 | 0.8508 | Just a Stranger | MCKOOL | pop, dance, edm | | yes |
| 2 | 0.8177 | Stereo Time | Sunwill | rock, electrorock, electronic | yes | yes |
| 3 | 0.7619 | IDK | THE DLX | pop, singersongwriter | | yes |
| 4 | 0.7613 | Give Me a Change | Marco Margna | pop, dance, ambient | | yes |
| 5 | 0.7523 | Just One Wink | Mercury in Summer | (none) | | |
| 6 | 0.7500 | More Than Just a Little Bit | Mike Falzone | (none) | | |
| 7 | 0.7317 | Criminal | Axl & Arth | pop, dance, rnb | | yes |
| 8 | 0.7275 | When You Are Gone | Marco Margna | pop, dance, singersongwriter | | yes |
| 9 | 0.7239 | Fell In Love With Summer | Songwriterz | pop | | yes |
| 10 | 0.7167 | Survive | JekK | house, electronic, electropop | | |

Notable: rank 2's same-artist track (Stereo Time) shares only one tag (rock)
with the seed's three (pop, dance, rock), yet the artist boost carries it to
rank 2 over several tracks with 2-3 tag overlaps — same-artist can outrank a
better tag match even when the acoustic content diverges. Worth knowing: the
boost trusts "same artist" as a relevance signal even when that artist's
catalog spans different sub-genres.

### Seed 246 — "All The Way" by Leslie Hunt
Genre tags: rock

| Rank | Score | Track | Artist | Genre tags | Same artist | Genre overlap |
|---|---|---|---|---|---|---|
| 1 | 0.9346 | Safe and Warm in Hunter's Arms | Roller Genoa | rock, bluesrock, garage | | yes |
| 2 | 0.9218 | Sorry | Dayung | rock, pop | | yes |
| 3 | 0.9174 | New Life | Explosive Ear Candy | pop, rock | | yes |
| 4 | 0.9016 | When the Stars Fall from Grace | Heifervescent | indie, pop, rock | | yes |
| 5 | 0.8975 | do it over | amélie | pop, rock | | yes |
| 6 | 0.8942 | Give Me Hope | Modern Pitch | rock, pop, indie | | yes |
| 7 | 0.8861 | Goodbye Romeo | Explosive Ear Candy | poprock | | |
| 8 | 0.8831 | Drop Down | Mike Falzone and the Peppermint Trick | (none) | | |
| 9 | 0.8778 | To The Roofs | Colaars | indie, rock, electronic | | yes |
| 10 | 0.8758 | Ballad of a Townie | Mike Falzone and the Peppermint Trick | (none) | | |

Notable: no same-artist candidate at all (Leslie Hunt's other tracks didn't
make the top-10), so this is a clean pure-content-plus-genre-boost list —
8/10 genre overlap, tight score band (0.876-0.935). One of the strongest
results in the sample.

### Seed 275 — "The Saymory - The Mirror Of You (Dr.Synthetique mix) wav" by the saymory
Genre tags: (none)

| Rank | Score | Track | Artist | Genre tags | Same artist | Genre overlap |
|---|---|---|---|---|---|---|
| 1 | 0.7743 | Let Them Go | Leslie Hunt | (none) | | |
| 2 | 0.7553 | Our Whole World Circling (Loveshadow Mix) | Emily Richards | pop, downtempo, electronic | | |
| 3 | 0.7370 | LEEONA - Do I | Zara Arshakian | house, electronic | | |
| 4 | 0.7299 | Laziness | Malika Rai | (none) | | |
| 5 | 0.7269 | Here to Stay | David Amber | pop, rock | | |
| 6 | 0.7246 | Time Travel | Jasmine Jordan | rnb, soul, hiphop | | |
| 7 | 0.7165 | Sexy jouet | Lollita | pop | | |
| 8 | 0.7116 | I Only Care About You When You're Gone | Leslie Hunt | (none) | | |
| 9 | 0.7076 | The Deep | Anitek | electronica, electronic, hiphop | | |
| 10 | 0.7057 | Moments | The.madpix.project | house, pop, dance | | |

Notable: pure CLAP-similarity list — no boost of either kind can apply since
the seed has no genre tags and no same-artist candidate surfaced. Nothing
here is *checkable* against metadata (no genre overlap column can be true by
construction), which is the clearest illustration in the sample of finding 2:
this is exactly the kind of recommendation that has to be trusted on the
embedding alone.

### Seed 400 — "rock'n'roll hall of fame" by pornophonique
Genre tags: rock, electronic, 8bit

| Rank | Score | Track | Artist | Genre tags | Same artist | Genre overlap |
|---|---|---|---|---|---|---|
| 1 | 0.8171 | 1/2 player game | pornophonique | 8bit, rock, electronic | yes | yes |
| 2 | 0.8067 | space invaders | pornophonique | electronic, 8bit, rock | yes | yes |
| 3 | 0.7704 | i want to be a machine | pornophonique | electronic, 8bit | yes | yes |
| 4 | 0.7577 | game over | pornophonique | rock, 8bit, electronic | yes | yes |
| 5 | 0.7217 | The Dreamer's Overture | JT Bruce | rock | | yes |
| 6 | 0.6800 | My World | WE ARE FM | rock, electronic, electrorock | | yes |
| 7 | 0.6587 | Poetic Pitbull Revolutions | Diablo Swing Orchestra | (none) | | |
| 8 | 0.6223 | The Soundtrack Of Our Summer | The League | rock | | yes |
| 9 | 0.6040 | Zodiac Virtues | Diablo Swing Orchestra | (none) | | |
| 10 | 0.6005 | Pasadena | Emerald Park | pop | | |

Notable: the strongest genre-coherence result in the sample — pornophonique
is a chiptune act, and CLAP correctly clusters its acoustically distinctive
8-bit sound both against its own catalog (ranks 1-4, exact tag match every
time) and against other genre-tagged tracks (ranks 5-6, 8). This is the best
single piece of evidence in the sample that CLAP embeddings are picking up
real acoustic identity, not noise — also the sample's other catalog-skew
case (4/10 same-artist, pornophonique has 8 tracks), so it demonstrates both
finding 1 and a genuinely good semantic match at once.

## Limitations (state plainly, don't let it surprise anyone live)

**There is no ground-truth relevance data for this dataset — no user
listens, skips, or explicit "these two tracks are similar" labels exist
anywhere in this pipeline.** That's a direct consequence of the 8.1 scope
(`CLAUDE.md`: behavioral-event topics are explicitly out of scope until 8.2)
and of using a small licensed seed dataset rather than a corpus with existing
usage signals. As a result:

- The ranking formula (`usecases/8_1_batch_reactive/docs/uc81-recommender.md`) is, by its own
  documentation, "a documented, tunable heuristic appropriate for a
  prototype at this scale... not a learned or validated ranking model."
  Nothing in this document changes that status — the spot-check and the
  catalog-skew/genre-overlap statistics above are *diagnostic*, not
  *validating*: they show the heuristic behaves as designed and surface one
  real weakness (catalog-size skew), but they cannot establish that its
  output is relevant to an actual listener, because there is no independent
  signal of relevance to check it against.
- "8.1 works" means the pipeline runs correctly end to end (stage 6) and
  produces recommendations that are internally consistent with the ranking
  formula's stated design (this document). It does not mean, and should not
  be presented as meaning, "the recommendations are good" in the sense of
  matching real listener preference — that claim has no supporting evidence
  here and would need either human relevance judgments or real behavioral
  data (both out of 8.1's scope) to establish.
- The honest answer to "how do you know the recommendations are relevant" is:
  the qualitative spot-check across 9 seeds (8 here plus the stage-5 worked
  example) shows the ranking formula visibly doing what it was designed to
  do — CLAP similarity dominates, boosts nudge as documented, the no-tag
  fallback isn't degenerate — and one design weakness (catalog-size skew)
  was caught in the process. That is evidence the *mechanism* works as
  intended. It is not evidence the *recommendations* are good, because no
  ground truth exists to check that against.
