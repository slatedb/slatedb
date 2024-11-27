# SlateDB Governance

Anyone is welcome to contribute to SlateDB by [creating pull requests](CONTRIBUTING.md)
or taking part in discussions about features, e.g. in [GitHub issues](https://github.com/slatedb/slatedb/issues).

This document defines who has authority to do what on the SlateDB open-source project.

While a formal assignment of authority is necessary as a fallback mechanism, it is
not the way decisions regarding SlateDB are usually made, or expected to be made.
The community sees disagreement as good as a diversity of perspectives having influence
leads to better decisions. We seek to resolve disagreement by reaching 
consensus based on the merit of technical arguments, with no regard to status or
formal role. This almost always works.

_This document does not apply to https://github.com/slatedb/slatedb-go._

# SlateDB Roles

There are two formal roles in SlateDB:

* __SlateDB Committers__ have the right to merge pull requests into SlateDB repositories,
  publish releases, and approve requests for comments (RFCs).
* __The SlateDB Board__ has final authority over all decisions made on the project.

# SlateDB Committers

A __SlateDB Committer__ is able to merge pull requests into SlateDB repositories under
[https://github.com/slatedb](https://github.com/slatedb), including into the main branches
that are released to production. Committers are also able to publish SlateDB releases and
approve RFCs. Approval requirements for these activities depends on the nature of the change:

- RFCs and release proposals must be approved by two committers other than the author of the
  request.
- Committers may merge small, low-risk, uncontroversial PRs without any approval.
- All other PRs must be approved by a SlateDB committer other than the author of the request.

Approval is indicated by clicking "approve" on a PR, by making a comment indicating approval,
or by simply merging a PR.

To become a SlateDB committer, start making PRs that are approved. When a sufficient
trust is built through working on SlateDB this way, any current SlateDB committer can nominate
you to the board for the committer role.

SlateDB's committers are listed in [slatedb-pulumi](https://github.com/slatedb/slatedb-pulumi/blob/main/__main__.py).

# The SlateDB Board

The __SlateDB Board__ has final authority over all decisions regarding the SlateDB project,
including:

* whether to merge (or keep) a PR in the event of any dispute, 
* who should be committers and who should be members of the board, and
* whether to make any changes to the governance policy (this document).

SlateDB's board members are:

* Vignesh Chandramohan (vigneshc)
* Rohan Desai (rodesai)
* Chris Riccomini (criccomini) - leader
* Li Yazhou (flaneur2020)

Board votes must be conducted on a Github issue, Github pull request, or Discord post. Votes
are represented as -1 and +1, with '-1' meaning 'no' and '+1' meaning 'yes.' Voting must be open
for at least 1 week or until all members have voted, whichever comes first. A vote passes if a
majority of the votes placed are in favor. On a tie, the leader of the board has a double vote.