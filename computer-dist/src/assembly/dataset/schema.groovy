// Define schema
schema.propertyKey("id").asInt().ifNotExist().create();
schema.propertyKey("rate").asDouble().ifNotExist().create();
schema.propertyKey("title").asText().ifNotExist().create();
schema.propertyKey("tag").asText().ifNotExist().create();
schema.propertyKey("genres").asText().valueSet().ifNotExist().create();
schema.propertyKey("timestamp").asText().ifNotExist().create();

schema.vertexLabel("user")
        .properties("id")
        .primaryKeys("id")
        .ifNotExist()
        .create();
schema.vertexLabel("movie")
        .properties("id", "title", "genres")
        .primaryKeys("id")
        .ifNotExist()
        .create();

schema.edgeLabel("rating")
        .sourceLabel("user")
        .targetLabel("movie")
        .properties("rate", "timestamp")
        .ifNotExist()
        .create();
schema.edgeLabel("taged")
        .sourceLabel("user")
        .targetLabel("movie")
        .properties("tag", "timestamp")
        .ifNotExist()
        .create();
