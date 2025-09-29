`graphite_to_mcap`
------------------

Simple system staticistics collection utility, which accepts data in Graphite
format (`<name> <value> <timestamp>`) and logs it to MCAP files.

- Input data:
    - Graphite format
      <https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol>
      is supported by many utilities, e.g.,
      <https://graphite.readthedocs.io/en/latest/tools.html>, but this tool is
      mainly intended to work with `collectd` <https://collectd.org/>.
    - Disable recommended packages when installing `collectd` in Ubuntu:
      otherwise it pulls too much junk, including Java.
    - `collectd` reports CPU usage in microseconds, percentage can be obtained
      by dividing the value by sampling interval and multiplying by 100. See
      https://github.com/collectd/collectd/issues/1633 and
      https://github.com/collectd/collectd/issues/2862.

- Output data:
    - MCAP <https://mcap.dev/> is handy for storing and analysing data, e.g.,
      with PlotJuggler <https://plotjuggler.io/>.
    - Data is wrapped in `plotjuggler_msgs`, but all corresponding dependencies
      are compiled in and there are no build or runtime dependencies on ROS.
