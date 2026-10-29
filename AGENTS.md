# Writing instructions

All writing must use ASD-STE100 Simplified Technical English: Linear tickets and comments, PR titles and descriptions, RFCs, review comments, commit messages, docs and code comments. Active voice, sentences under 30 words, two ideas per sentence at most, no filler.

When you write a pull request description, you always follow `.github/pull_request_template.md`.

You line-wrap comments and prose at 100 characters.

## Brevity

You follow the spirit of Churchill's brevity memorandum when writing pull requests descriptions and documentation. The memorandum is reproduced below.

To do our work, we all have to read a mass of papers. Nearly all of them are far too long. This wastes time, while energy has to be spent in looking for the essential points.

I ask my colleagues and their staffs to see to it that their Reports are shorter.

1. The aim should be Reports which set out the main points in a series of short, crisp paragraphs.
2. If a Report relies on detailed analysis of some complicated factors, or on statistics, these should be set out in an Appendix.
3. Often the occasion is best met by submitting not a full-dress Report, but an Aide-memoire consisting of headings only, which can be expanded orally if needed.
4. Let us have an end of such phrases as these: “It is also of importance to bear in mind the following considerations…”, or “Consideration should be given to the possibility of carrying into effect…”. Most of there woolly phrases are mere padding, which can be left out altogether , or replaced by a single word. Let us not shrink from using the short expressive phrase, even if it is conversational.

Reports drawn up on the lines I propose may at first seem rough as compared with the flat surface of officialese jargon. But the saving in time will be great, while the discipline of setting out the real points concisely will prove an aid to clearer thinking.

# API design

Do not add `#[non_exhaustive]` to an enum only to avoid a future breaking change.
Prefer exhaustive enum matches, even when a new variant causes a breaking API change.
