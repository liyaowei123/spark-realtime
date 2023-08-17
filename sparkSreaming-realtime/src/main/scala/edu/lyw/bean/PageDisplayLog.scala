package edu.lyw.bean

case class PageDisplayLog(
                           mid: String,
                           userId: String,
                           provinceId: String,
                           channel: String,
                           isNew: String,
                           model: String,
                           operateSystem: String,
                           versionCode: String,
                           brand: String,
                           pageId: String,
                           lastPageId: String,
                           pageItem: String,
                           pageItemType: String,
                           duringTime: Long,
                           sourceType: String,
                           displayType: String,
                           displayItem: String,
                           displayItem_type: String,
                           displayOrder: String,
                           displayPosId: String,
                           ts: Long
                         )
