package edu.lyw.bean

case class StartLog(
                     mid: String,
                     userId: String,
                     provinceId: String,
                     channel: String,
                     isNew: String,
                     model: String,
                     operateSystem: String,
                     versionCode: String,
                     entry: String,
                     openAdId: String,
                     loadingTimeMs: Long,
                     openAdMs: Long,
                     openAdSkipMs: Long,
                     ts: Long
                   )
