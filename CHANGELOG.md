# Changelog

## 1.0.0
  * Beta release changes [#23](https://github.com/singer-io/tap-iterable/pull/23)
    * Discovery and API request fixes [#20](https://github.com/singer-io/tap-iterable/pull/20)
      * Add api_key in header
      * Replication key changes of `users` stream to `profileUpdatedAt` from `createdAt`
      * Add inclusion="automatic" for replication keys in the schema
    * Add missing fields in schema [#21](https://github.com/singer-io/tap-iterable/pull/21) 
    * Bookmarking related changes [#17](https://github.com/singer-io/tap-iterable/pull/17)
      * Changes in behaviour of `templates` and `campaigns` streams to work as INCREMENTAL (TDL-22206)
      * Fix to parse the response of list_users stream properly (TDL-22213)
      * Fix for the bug where all records were not getting fetched in emails related streams (TDL-22208)
      * Make the bookmark dates format uniform across all the streams (TDL-22210)
      * Bookmarking logic bugfix (TDL-22209, TDL-22211)
      * Add support for interrupted sync (TDL-22157)
    * Date windowing logic change (TDL-22161) [#18](https://github.com/singer-io/tap-iterable/pull/18)
    * Implementation of constant backoff and exception handling logic [#19](https://github.com/singer-io/tap-iterable/pull/19)
    * Add the integration test case scenarios [#22](https://github.com/singer-io/tap-iterable/pull/22)   
    * Add the unit test cases
    * Update in config.yml
 