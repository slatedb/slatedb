# Verification checklist

Run this pass on every draft before you deliver it. The checks are ordered from mechanical to judgment.

## Mechanical checks (searchable)

Search the draft for each pattern. Every hit outside code blocks and quoted text is a violation.

| Search for | Violation | Fix |
|---|---|---|
| `'ll`, `'re`, `'ve`, `n't`, `it's` | Contraction (Rule 4.2) | Expand it. |
| `has been`, `have been`, `had been` | Present/past perfect (Rule 3.4) | Simple past or simple present. |
| `has` / `have` + past participle | Present perfect (Rule 3.4) | Simple past. |
| `should`, `would`, `may`, `might`, `could`, `shall` | Unapproved modal (Rule 3.2) | See the modal ladder in rule-catalog.md. |
| `is being`, `are being`, `was being` | Progressive passive (Rules 3.4, 3.5) | Active, simple tense. |
| `, making`, `, allowing`, `, enabling`, `, ensuring` | "-ing" clause as verb (Rule 3.5) | New sentence with a real subject. |
| `;` | Semicolon (Rule 8.1) | Two sentences. |
| `—`, `–`, or ` - ` / ` -- ` between two statements | Dash: implied logic junction (skill check, Section 8). Not a violation: a dash that identifies a list item (Rule 4.3), a CLI flag (`--force`), a range (`5 - 10`) | Name the relation ("because", "but", "for example", "that is"), or write two sentences. |
| `e.g.`, `i.e.`, `etc.` | Latin abbreviation (GR-6) | "for example", "that is", name the items. |
| `simply`, `easily`, `seamlessly`, `robust` | Filler (no fact) | Delete. |
| `delve`, `pivotal`, `crucial`, `leverage`, `showcase`, `foster` | LLM-tell words (word-swaps.md) | Use the listed replacement, or delete. |
| ` if `, ` when ` (mid-sentence) | Trailing condition (Rule 5.4) | Move the condition to the start of the sentence, add a comma. |
| `however`, `therefore`, `since` (= because), `now` | Recurring errors (dictionary introduction) | but / thus, as a result / because / at this time (better, delete) |
| `need to`, `have to` | Recurring errors | Imperative in procedures; `it is necessary to` in descriptive text |
| `perform`, `insert`, `reach`, `avoid`, `repeat`, `acceptable` | Recurring errors | do / put / get, get to / prevent / do … again / permitted |
| `the example below`, `the section above` | "below" and "above" as adverbs are not approved | Name the target, or write `…that follows` |
| ` is complete`, ` are complete` | "complete" as an adjective is not approved | completed (adjective), or full / all |

## Countable checks

1. **Sentence length.** Count words in each sentence. Procedural limit: 20. Descriptive limit: 25. Notes: 25.
   Backticked commands, numbers with units, and identifiers count as one word each (Rule 8.6).
   In a vertical list, the lead-in colon ends a sentence and each item that follows counts as a new sentence with its own budget (Rule 8.4).
2. **Paragraph size.** Maximum six sentences per paragraph (Rule 6.6).
3. **Multi-word nouns.** Any noun chain over three words → break it with prepositions (Rule 2.1).
4. **Instructions per sentence.** One, unless the actions are simultaneous (Rule 5.2).
5. **List mechanics.** Colon on the lead-in. Each item starts with an uppercase letter. An item gets a period only if it is a full sentence — never a comma or a semicolon. The last item gets a period. No nested lists. Instructions and facts never in the same list (Rule 4.3).

## Judgment checks

6. **Classification.** Is each passage cleanly procedural or descriptive? Procedures in imperative, descriptions never in imperative.
7. **Voice.** Any passive sentence: is the agent truly unknown, and is the passage descriptive? Otherwise make it active (Rule 3.6). For an unknown agent, prefer "you" (the reader) or "we" (your company) over the passive.
8. **Condition placement.** Every "if/when" stands before its command, with a comma (Rule 5.4).
9. **Synonym rotation.** One term per concept across the whole document (Rules 1.11, 9.4). Scan for check/verify/confirm, config/settings, run/execute.
10. **Warnings.** Command or condition first, risk second (Rules 7.2, 7.3). If a passage risks both injury and damage, use WARNING (Rule 7.1).
11. **Limits with actions.** A result or limit comes directly after its action in the work step — not in a note (Rules 5.2, 5.5).
12. **Notes test.** Delete all notes, then read the procedure. The reader must still be able to do it correctly (Rule 5.5).
13. **Completeness.** Articles present, "that" present after "make sure", no telegraph style (Rule 4.2).
14. **Plain words.** Each technical term has a definition at its first use. Common words replaced jargon where a common word exists.
15. **Strict mode only.** Run the two tables in `references/strict-vocabulary.md` against the draft.
16. **Untouchables intact.** Code, identifiers, quoted errors, UI labels, and proper nouns are unchanged.

## When reporting violations (check mode)

For each violation give: the rule number, the offending text, and a compliant rewrite. Cite only rule numbers that appear in rule-catalog.md.
End the report with this statement, one time per conversation, when the user asked for STE compliance: "No tool can guarantee ASD-STE100 compliance. Final approval rests with the writer. The official standard is a free download at asd-ste100.org."
