// 电影属性
schema.propertyKey("名称").asText().ifNotExist().create();
schema.propertyKey("类型").asText().valueSet().ifNotExist().create();
schema.propertyKey("发行时间").asDate().ifNotExist().create();

// 演出角色
schema.propertyKey("角色").asText().ifNotExist().create();

// 电影
schema.vertexLabel("电影").useCustomizeStringId()
      .properties("名称","类型","发行时间")
      .nullableKeys("类型","发行时间")
      .ifNotExist()
      .create();
// 艺人
schema.vertexLabel("艺人").useCustomizeStringId().ifNotExist().create();
// 类型
schema.vertexLabel("类型").useCustomizeStringId().ifNotExist().create();
// 年份
schema.vertexLabel("年份").useCustomizeStringId().ifNotExist().create();

// 导演: 艺人 -> 电影
schema.edgeLabel("导演").link("艺人","电影").ifNotExist().create();

// 演出: 艺人 -> 电影
schema.edgeLabel("演出").link("艺人","电影")
      .properties("角色")
      .nullableKeys("角色")
      .ifNotExist().create();

// 属于: 电影 -> 类型
schema.edgeLabel("属于").link("电影","类型").ifNotExist().create();

// 发行于: 电影 -> 年份
schema.edgeLabel("发行于").link("电影","年份").ifNotExist().create();
