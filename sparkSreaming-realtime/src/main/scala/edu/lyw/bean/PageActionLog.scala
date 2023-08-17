package edu.lyw.bean

case class PageActionLog(
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
                          action_id:String,
                          action_item:String,
                          action_item_type:String,
                          ts:Long
                        )
