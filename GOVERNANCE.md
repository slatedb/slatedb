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

# SlateDB Roles

There are two formal roles in SlateDB:

* __SlateDB Committers__ have the right to merge pull requests into the SlateDB repo.
* __The SlateDB Board__ has final authority over all decisions made on the project.

# SlateDB Committers

A __SlateDB Committer__ is able to merge pull requests into the SlateDB repositories under
[https://github.com/slatedb](https://github.com/slatedb), including into
the master branch that is released to production.

To become a SlateDB Committer, start making PRs that are approved. When a sufficient
trust is built through working on SlateDB this way, any current SlateDB Committer can nominate
you to the board for the committer role.

Every pull request should be approved by a SlateDB Committer other than the author of the
request by clicking "approve", by making a comment indicating agreement, or by simply
merging it. SlateDB Committers may merge their own PRs without approval from another committer
if the PR is small, low-risk, and uncontroversial. This is left to the discretion of the
committer.

The SlateDB committers are listed in [slatedb-pulumi](https://github.com/slatedb/slatedb-pulumi/blob/main/__main__.py).

# The SlateDB Board

The __SlateDB Board__ has final authority over all decisions regarding the SlateDB project,
including:

* whether to merge (or keep) a PR in the event of any dispute, 
* who should be committers and who should be members of the board, and
* whether to make any changes to the governance policy (this document).

Board members need not be committers, but they can be. Similarly, committers need not be board
members, but they can be.

Currently, the board consists of each of the SlateDB committers.
[Chris Riccomini](https://github.com/criccomini) is the board leader. On a tie, the leader of
the board has a double vote.
