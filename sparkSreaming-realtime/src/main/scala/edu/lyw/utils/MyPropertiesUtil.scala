package edu.lyw.utils

import java.util.ResourceBundle

object MyPropertiesUtil {
  val bundle: ResourceBundle = ResourceBundle.getBundle("config")
  def apply(key: String) = bundle.getString(key)
}
