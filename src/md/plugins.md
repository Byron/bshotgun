All plugins described here may be used within the [be UCT](http://byron.github.io/bcore/be/).

## be Shotgun

The **shotgun** command makes commands for initializing our ORM's caches and update them.

![under construction](https://raw.githubusercontent.com/Byron/bcore/master/src/images/wip.png)

### Caveats

* Unless specified differently, all file operations are additive. This means that it will never remove files, even though they wouldn't be needed anymore. When updating caches, you ideally remove the existing files to make sure there are no left-overs. However, failing to do so means no harm either.
* Updating the SQL cache has the danger of missing edits, especially in busy databases. **TODO: write about the auto-update workflow with shotgun-events, and how to time this operation (maybe use event replay to put in what was missed afterwards)**

## be Shotgun-Tests

* Utility to initialize a sample database for use in test-cases. It has built-in support for scrambling strings to keep the data private, while being in a public spot.