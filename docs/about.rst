About collect
================================================================

The collect library was developed by MBK Engineers to gather web-scraping utilities for public data relevant to water resources engineering. It provides an interface to query USGS, CDEC, CNRFC, USACE, and other web sources, returning data as a pandas.DataFrame and gage metadata as a dictionary.


Project Source Code
----------------------------------------
https://github.com/MBKEngineers/collect


Contributing
----------------------------------------
Report bugs and/or new feature requests with the GitHub issues tracker: https://github.com/MBKEngineers/collect/issues

To start development on a new feature, pull the latest state of **main** and checkout a new branch (ex: **dev**)::

   $ git fetch
   $ git checkout main
   $ git pull origin main
   $ git checkout -b dev

To push your branch to the GitHub **collect** repository::

   $ git push origin dev

For contributing code, open a pull request from your **dev** branch into **main** or the target branch and request a review.  Before requesting review, make sure that the remote (GitHub) version of **main** is merged into your local **dev** branch::

   $ git fetch
   $ git merge remotes/origin/main

Resolve any conflicts and request a review.  Additional changes may be made to the open pull request (PR) by pushing to the **dev** branch on GitHub.  Once review comments have been addressed and the PR is approved, you may merge your branch into main and delete the **dev** branch.


Updating Documentation
----------------------------------------
The `collect` module uses Sphinx to generate documentation from doc-strings in the project.  To update and access documentation files, make sure that Sphinx is installed::

   $ pip install -r requirements.txt


The `collect/doc/_build` folder is not (yet) tracked in the repository.  To make the documentation, change directory into `collect/doc` and run the `make` command::

   $ cd doc
   $ make html
   $ cd _build/html
   $ open index.html

This should open the new documention index page in your default browser.