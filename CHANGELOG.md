# Changelog

## 2.1.0
  * Streams that the credentials cannot access are excluded from the catalog during discovery. [#34](https://github.com/singer-io/tap-iterable/pull/34)
  * Added unit tests for discovery access-check logic

## 2.0.0
  * Fix bookmarking issue in template stream and integration tests by improving backoff strategy [#31](https://github.com/singer-io/tap-iterable/pull/31)
  * Add logic for 5xx retry to handle server 5xx errors retries [#33](https://github.com/singer-io/tap-iterable/pull/33)
  * Adds parent-tap-stream-id field to catalog for child streams [#30](https://github.com/singer-io/tap-iterable/pull/30)

## 1.0.2
* Dependency upgrades [#28](https://github.com/singer-io/tap-iterable/pull/28)

## 1.0.1
  * Made api_window_in_days as not required in config [25](https://github.com/singer-io/tap-iterable/pull/25)

## 1.0.0
  * Beta release changes [#23](https://github.com/singer-io/tap-iterable/pull/23)
    * Discovery and API request fixes [#20](https://github.com/singer-io/tap-iterable/pull/20)
      * Add api_key in header
      * Replication key changes of `users` stream to `profileUpdatedAt` from `createdAt`
      * Add inclusion="automatic" for replication keys in the schema
    * Add missing fields in schema [#21](https://github.com/singer-io/tap-iterable/pull/21)
    * Bookmarking related changes [#17](https://github.com/singer-io/tap-iterable/pull/17)
      * Changes in behaviour of `templates` and `campaigns` streams to work as INCREMENTAL
      * Fix to parse the response of list_users stream properly
      * Fix for the bug where all records were not getting fetched in emails related streams
      * Make the bookmark dates format uniform across all the streams
      * Bookmarking logic bugfix
      * Add support for interrupted sync
    * Date windowing logic change [#18](https://github.com/singer-io/tap-iterable/pull/18)
    * Implementation of constant backoff and exception handling logic [#19](https://github.com/singer-io/tap-iterable/pull/19)
    * Add the integration test case scenarios [#22](https://github.com/singer-io/tap-iterable/pull/22)
    * Add the unit test cases
    * Update in config.yml
