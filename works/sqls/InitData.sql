insert into bfb_t_param_config_list values ('kafka.application.id','KafkaStreamsTopicFilter');

insert into bfb_t_param_config_list values ('kafka.group1.source.topic','');
insert into bfb_t_param_config_list values ('kafka.group1.regex.string','');
insert into bfb_t_param_config_list values ('kafka.group1.target.topic','');

insert into bfb_t_param_config_list values ('kafka.bootstrap.servers','20.0.0.29:21005,20.0.0.30:21005,20.0.0.31:21005,20.0.0.32:21005,20.0.0.33:21005');

insert into bfb_t_param_config_list values ('kafka.consumer.topics','');

insert into bfb_t_param_config_list values ('spark.streaming.application.name','SparkStreamingKafkaToDatabase');
insert into bfb_t_param_config_list values ('spark.streaming.batch.duration','5');
insert into bfb_t_param_config_list values ('spark.streaming.checkpoint.dir','nocp');

insert into bfb_t_param_config_list values ('spark.topics.topic1.process.class','com.service.data.spark.streaming.process.DefaultTextProcess');
insert into bfb_t_param_config_list values ('spark.topics.topic2.process.class','com.service.data.spark.streaming.process.DefaultJsonProcess');
