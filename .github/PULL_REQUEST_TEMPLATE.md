<!-- 
  Thank you very much for contributing to Apache HugeGraph, we are happy that you want to help us improve HugeGraph! 

  Here are some tips for you:

    1. If this is your first time, please read the [contributing guidelines](https://github.com/apache/incubator-hugegraph/blob/f23c648937c8c4601a9f721d688eabfec7a8cfb3/CONTRIBUTING.md).
  
    2. Replace texts surrounded by `<` and `>` based on your pull request, e.g. <ISSUE NUMBER>.

    3. Name the pull request in the form "[<TYPE>-<ISSUE NUMBER>] [<COMPONENT>] Title of the pull request", 
      where <TYPE> should be clear, such as: Feature | Fix | Improvement | Refactor | Revert | Chore, 
      <ISSUE NUMBER> should be replaced by the actual issue number.   
      Skip <COMPONENT> if you are unsure about which is the best component.

    4. Each pull request should address only one issue, not mix up code from multiple issues.

    5. Put an `x` in the `[ ]` to mark the item as CHECKED. `[x]`
-->

## Purpose of the pull request

- This PR will close <ISSUE NUMBER>

<!-- Please explain more context in this section, clarify why the changes are needed. 

For example:

- If you propose a new API, clarify the use case for a new API.
- If you fix a bug, you can clarify why it is a bug. -->

## Summary of changes

<!-- Please clarify what changes you are proposing. The purpose of this section is to outline the changes and how this PR fixes the issue. These change logs are helpful for better ant faster reviews.)

For example:

- If you introduce a new feature, please show detailed design here or add the link of design documentation.
- If you refactor some codes with changing classes, showing the class hierarchy will help reviewers.
- If there is a discussion in the mailing list, please add the link. -->

## Verifying these changes

<!-- Please pick either of the following options -->

- This change is a trivial rework / code cleanup without any test coverage.

*(or)*

- This change is already covered by existing tests, such as <PLEASE DESCRIBE TESTS>.

*(or)*

- This change added tests and can be verified as follows:
  
  *(for example:)*
  - *Added integration tests for end-to-end deployment with large payloads (10MB)*
  - *Manually verified the change by running a HugeGraphComputerJob with some network exception, verifying that Worker shuts down and job is cancelled.*


## Does this pull request potentially affect one of the following parts:

<!-- DO NOT REMOVE THIS SECTION. CHECK THE PROPER BOX ONLY. -->

- [ ]  Dependencies (add or upgrade a dependency, do not forget to add license)
- [ ]  The public API
- [ ]  The default values of configurations
- [ ]  The CRD generation script
- [ ]  The Kubernetes Operator
- [ ]  Anything that affects deployment

## Documentation

<!-- DO NOT REMOVE THIS SECTION. CHECK THE PROPER BOX ONLY. -->

- [ ]  `doc-required` <!-- Your PR changes impact docs and you will update later -->
- [ ]  `doc-not-needed` <!-- Your PR changes do not impact docs -->
- [ ]  `doc-complete` <!-- Related docs have been already added or updated -->
