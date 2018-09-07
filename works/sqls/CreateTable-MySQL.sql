drop table if exists bfb_t_param_config_list;

drop table if exists bfb_t_topic_table_list;

drop table if exists bfb_t_table_column_list;

drop table if exists bfb_t_topic_code_mapping;

/*==============================================================*/
/* Table: bfb_t_param_config_list                               */
/*==============================================================*/
create table bfb_t_param_config_list
(
   param_name           varchar(128) not null comment '参数名称',
   param_value          varchar(128) comment '参数值',
   primary key (param_name)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 comment = '参数配置表';

alter table bfb_t_param_config_list comment '参数配置表';

/*==============================================================*/
/* Table: bfb_t_topic_table_list                                */
/*==============================================================*/
create table bfb_t_topic_table_list
(
   topic_name           varchar(128) not null comment '主题名称',
   topic_desc           varchar(128) comment '主题描述',
   table_name           varchar(32) comment '目标表名称',
   field_split          varchar(32) comment '字段分隔符',
   primary key (topic_name)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 comment = '主题配置表';

alter table bfb_t_topic_table_list comment '主题配置表';

/*==============================================================*/
/* Table: bfb_t_table_column_list                               */
/*==============================================================*/
create table bfb_t_table_column_list
(
   table_name           varchar(32) not null comment '目标表名称',
   table_desc           varchar(128) comment '表描述',
   column_name          varchar(32) not null comment '字段名称',
   column_desc          varchar(128) comment '字段描述',
   column_type          varchar(32) comment '字段类型',
   column_index         int comment '字段顺序',
   column_data          varchar(128) comment '字段值',
   primary key (table_name, column_name)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 comment = '表结构配置表';

alter table bfb_t_table_column_list comment '表结构配置表';

/*==============================================================*/
/* Table: bfb_t_topic_code_mapping                              */
/*==============================================================*/
create table bfb_t_topic_code_mapping
(
   topic_name           varchar(128) not null comment '主题名称',
   column_name          varchar(32) not null comment '字段名称',
   source_code          varchar(32) not null comment '源代码',
   source_code_desc     varchar(128) comment '源代码描述',
   target_code          varchar(32) comment '目标代码',
   target_code_desc     varchar(128) comment '目标代码描述',
   primary key (topic_name, column_name, source_code)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 comment = '码值转换配置表';

alter table bfb_t_topic_code_mapping comment '码值转换配置表';
