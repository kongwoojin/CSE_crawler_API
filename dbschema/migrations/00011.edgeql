CREATE MIGRATION m1iayblseici3hl6522vxlllfrdmyz2eyq6eouzdi4nvv6r2t2umza
    ONTO m17a22nzvio7om4d5fwjccyfpienbi2vjfqhy7a2pe63rhvvxvn6ra
{
  DROP TYPE default::arch;
  DROP TYPE default::cse;
  DROP TYPE default::dorm;
  DROP TYPE default::emc;
  DROP TYPE default::ide;
  DROP TYPE default::ite;
  DROP TYPE default::mechanical;
  DROP TYPE default::mechatronics;
  DROP TYPE default::school;
  CREATE TYPE default::notice {
      CREATE MULTI LINK files: default::Files {
          ON TARGET DELETE ALLOW;
      };
      CREATE REQUIRED PROPERTY article_url: std::str {
          CREATE CONSTRAINT std::exclusive;
      };
      CREATE REQUIRED PROPERTY board: std::str;
      CREATE REQUIRED PROPERTY content: std::str;
      CREATE REQUIRED PROPERTY init_crawled_time: cal::local_datetime;
      CREATE REQUIRED PROPERTY is_notice: std::bool;
      CREATE REQUIRED PROPERTY notice_end_date: cal::local_date;
      CREATE REQUIRED PROPERTY notice_start_date: cal::local_date;
      CREATE REQUIRED PROPERTY num: std::str;
      CREATE REQUIRED PROPERTY read_count: std::int64;
      CREATE REQUIRED PROPERTY title: std::str;
      CREATE REQUIRED PROPERTY update_crawled_time: cal::local_datetime;
      CREATE REQUIRED PROPERTY write_date: cal::local_date;
      CREATE REQUIRED PROPERTY writer: std::str;
  };
  DROP TYPE default::sim;
};
