# SlateDB Governance

This document defines who has authority to do what on the SlateDB open-source project.

Anyone is welcome to participate in SlateDB by opening
[GitHub issues](https://github.com/slatedb/slatedb/issues) or taking part in
[Discord](https://discord.gg/mHYmGy5MgA) discussions. You must become a contributor to
submit pull requests.

While a formal assignment of authority is necessary as a fallback mechanism, it is
not the way decisions regarding SlateDB are usually made, or expected to be made.
The community sees disagreement as good as a diversity of perspectives having influence
leads to better decisions. We seek to resolve disagreement by reaching 
consensus based on the merit of technical arguments, with no regard to status or
formal role. This almost always works.

# SlateDB Roles

There are three formal roles in SlateDB:

* __SlateDB Contributors__ have the right to submit pull requests to SlateDB repositories.
* __SlateDB Committers__ have the right to merge pull requests into SlateDB repositories,
  publish releases, and approve requests for comments (RFCs).
* __The SlateDB Board__ has final authority over all decisions made on the project.

# SlateDB Contributors

A __SlateDB Contributor__ is able to submit pull requests to SlateDB repositories under
[https://github.com/slatedb](https://github.com/slatedb). Contributors must follow the
[CLEAN_SLATE.md](CLEAN_SLATE.md) style guide and the [CONTRIBUTING.md](CONTRIBUTING.md)
contributor's guide.

To become a SlateDB contributor, you must demonstrate high-quality interactions in the
community, including:

- Opening issues that are well-researched and clearly written.
- Demonstrating constructive AI usage and avoiding slop bombs.
- Participating in SlateDB's Discord and GitHub communities a constructive and respectful
  manner.

When a sufficient trust is built through participating in the SlateDB community in this way,
any current SlateDB committer can add you as a `Read`-role collaborator on the SlateDB GitHub
repository.

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
