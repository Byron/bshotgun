This page describes shotgun from the point of view of a developer, providing certain insights that should help dealing with it.

![under construction](https://raw.githubusercontent.com/Byron/bcore/master/src/images/wip.png)

## TODO

* Pipeline steps are entities !
    + linked to the entity type they apply to
    + GUI exposes them through special interface only, it's impossible to see the step entity using the web-frontend, which would be useful to add custom fields for use in tank templates.
* Probably SQL-Based object-oriented datamodel, with support for dynamic fields and doubly-linked connections between fields. Usually for each link, there is a back link (verify this)
    + Entities are used consistently within shotgun, there is no magic as everything is stored in some sort of entity object.


## Issues

* Every single field of the ~2500 ones in shotgun should have a description saying what's its purpose, but you will see this just isn't the case unless you are wise enough to do that for the ones you add yourself.
* Field names are inconsistent, probably as many of them have been created in the early days without giving it much thought.
    + new fields are always prefixed with `sg_`, if sg added them, as well as if you did.
    - old fields are usually, but not always not having a prefix at all.
