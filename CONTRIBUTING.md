## How to contribute

Contributors are welcome to submit their code and ideas. In a long run, we hope this project can be managed by developers from both inside and outside Alibaba.

### Before contributing

* Sign CLA of PolarDB-X:
  Please download PolarDB-X [CLA](https://gist.github.com/alibaba-oss/151a13b0a72e44ba471119c7eb737d74). Follow the instructions to sign it.

Here is a checklist to prepare and submit your PR (pull request).

* Create your own Github branch by forking PolarDB-X related repositories.
* Checkout [README](README.md) for how to start PolarDB-X from source code.
* Push changes to your personal fork and make sure they follow our coding style (descriped in each repository)
* Create a PR with a detail description, if commit messages do not express themselves.
* Submit PR for review and address all feedbacks.
* Wait for merging (done by committers).

Let's use an example to walk through the list.

## An Example of Submitting Code Change to PolarDB-X

### Fork Your Own Branch

There are many PolarDB-X related repositories, take ApasaraDB GalaxySQL and ApasaraDB GalaxyGlue for an example. On Github page of [GalaxySQL](https://github.com/apsaradb/galaxysql) and [GalaxyGlue](https://github.com/apsaradb/galaxyglue), Click **fork** button to create your own galaxysql and galaxyglue repository.

### Create Local Repository
```bash
git clone https://github.com/your_github/galaxysql.git
cd galaxysql
git submodule add https://github.com/your_github/galaxyglue.git polardbx-rpc
```
### Create a dev Branch (named as your_github_id_feature_name)
```bash
git branch your_github_id_feature_name
```
### Make Changes and Commit Locally
```bash
git status
git add files-to-change
git commit -m "messages for your modifications"
```

### Rebase and Commit to Remote Repository
```bash
git checkout develop
git pull
git checkout your_github_id_feature_name
git rebase develop
-- resolve conflict, compile and test --
git push --recurse-submodules=on-demand origin your_github_id_feature_name
```

### Create a PR
Click **New pull request** or **Compare & pull  request** button, choose to compare branches apsaradb/galaxysql and your_github/your_github_id_feature_name, and write PR description.

### Address Reviewers' Comments
Resolve all problems raised by reviewers and update PR.

### Merge
It is done by PolarDB-X committers.
___

Copyright © Alibaba Group, Inc.