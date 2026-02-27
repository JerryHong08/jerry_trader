import "strings"

from(bucket: "jerrymmm")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "trade_activity")
  |> filter(fn: (r) => exists r.session_id)
  |> map(fn: (r) => {
      sid = string(v: r.session_id)

      return {
        r with
        date:
          strings.substring(v: sid, start: 0, end: 4) + "-" +
          strings.substring(v: sid, start: 4, end: 6) + "-" +
          strings.substring(v: sid, start: 6, end: 8),
        mode:
          strings.substring(
            v: sid,
            start: 9,
            end: strings.strlen(v: sid)
          )
      }
  })
  |> drop(columns: ["session_id"])
  |> to(bucket: "jerrymmm_v2")
