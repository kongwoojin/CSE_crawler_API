CREATE MIGRATION m1dsbopymwhrltb5oc2bbyer3afwe67hnnzxr2lvvoifha4ndgztjq
    ONTO initial
{
  CREATE TYPE default::arch {
      CREATE REQUIRED PROPERTY files -> array<tuple<name: std::str, url: std::str>>;
      CREATE REQUIRED PROPERTY article_url -> std::str;
      CREATE REQUIRED PROPERTY content -> std::str;
      CREATE REQUIRED PROPERTY crawled_time -> cal::local_datetime;
      CREATE REQUIRED PROPERTY num -> std::str;
      CREATE REQUIRED PROPERTY read_count -> std::int64;
      CREATE REQUIRED PROPERTY title -> std::str;
      CREATE REQUIRED PROPERTY write_date -> cal::local_date;
      CREATE REQUIRED PROPERTY writer -> std::str;
  };
  CREATE TYPE default::cse {
      CREATE REQUIRED PROPERTY files -> array<tuple<name: std::str, url: std::str>>;
      CREATE REQUIRED PROPERTY article_url -> std::str;
      CREATE REQUIRED PROPERTY content -> std::str;
      CREATE REQUIRED PROPERTY crawled_time -> cal::local_datetime;
      CREATE REQUIRED PROPERTY num -> std::str;
      CREATE REQUIRED PROPERTY read_count -> std::int64;
      CREATE REQUIRED PROPERTY title -> std::str;
      CREATE REQUIRED PROPERTY write_date -> cal::local_date;
      CREATE REQUIRED PROPERTY writer -> std::str;
  };
  CREATE TYPE default::dorm {
      CREATE REQUIRED PROPERTY files -> array<tuple<name: std::str, url: std::str>>;
      CREATE REQUIRED PROPERTY article_url -> std::str;
      CREATE REQUIRED PROPERTY content -> std::str;
      CREATE REQUIRED PROPERTY crawled_time -> cal::local_datetime;
      CREATE REQUIRED PROPERTY num -> std::str;
      CREATE REQUIRED PROPERTY read_count -> std::int64;
      CREATE REQUIRED PROPERTY title -> std::str;
      CREATE REQUIRED PROPERTY write_date -> cal::local_date;
      CREATE REQUIRED PROPERTY writer -> std::str;
  };
  CREATE TYPE default::emc {
      CREATE REQUIRED PROPERTY files -> array<tuple<name: std::str, url: std::str>>;
      CREATE REQUIRED PROPERTY article_url -> std::str;
      CREATE REQUIRED PROPERTY content -> std::str;
      CREATE REQUIRED PROPERTY crawled_time -> cal::local_datetime;
      CREATE REQUIRED PROPERTY num -> std::str;
      CREATE REQUIRED PROPERTY read_count -> std::int64;
      CREATE REQUIRED PROPERTY title -> std::str;
      CREATE REQUIRED PROPERTY write_date -> cal::local_date;
      CREATE REQUIRED PROPERTY writer -> std::str;
  };
  CREATE TYPE default::ide {
      CREATE REQUIRED PROPERTY files -> array<tuple<name: std::str, url: std::str>>;
      CREATE REQUIRED PROPERTY article_url -> std::str;
      CREATE REQUIRED PROPERTY content -> std::str;
      CREATE REQUIRED PROPERTY crawled_time -> cal::local_datetime;
      CREATE REQUIRED PROPERTY num -> std::str;
      CREATE REQUIRED PROPERTY read_count -> std::int64;
      CREATE REQUIRED PROPERTY title -> std::str;
      CREATE REQUIRED PROPERTY write_date -> cal::local_date;
      CREATE REQUIRED PROPERTY writer -> std::str;
  };
  CREATE TYPE default::ite {
      CREATE REQUIRED PROPERTY files -> array<tuple<name: std::str, url: std::str>>;
      CREATE REQUIRED PROPERTY article_url -> std::str;
      CREATE REQUIRED PROPERTY content -> std::str;
      CREATE REQUIRED PROPERTY crawled_time -> cal::local_datetime;
      CREATE REQUIRED PROPERTY num -> std::str;
      CREATE REQUIRED PROPERTY read_count -> std::int64;
      CREATE REQUIRED PROPERTY title -> std::str;
      CREATE REQUIRED PROPERTY write_date -> cal::local_date;
      CREATE REQUIRED PROPERTY writer -> std::str;
  };
  CREATE TYPE default::mechanical {
      CREATE REQUIRED PROPERTY files -> array<tuple<name: std::str, url: std::str>>;
      CREATE REQUIRED PROPERTY article_url -> std::str;
      CREATE REQUIRED PROPERTY content -> std::str;
      CREATE REQUIRED PROPERTY crawled_time -> cal::local_datetime;
      CREATE REQUIRED PROPERTY num -> std::str;
      CREATE REQUIRED PROPERTY read_count -> std::int64;
      CREATE REQUIRED PROPERTY title -> std::str;
      CREATE REQUIRED PROPERTY write_date -> cal::local_date;
      CREATE REQUIRED PROPERTY writer -> std::str;
  };
  CREATE TYPE default::mechatronics {
      CREATE REQUIRED PROPERTY files -> array<tuple<name: std::str, url: std::str>>;
      CREATE REQUIRED PROPERTY article_url -> std::str;
      CREATE REQUIRED PROPERTY content -> std::str;
      CREATE REQUIRED PROPERTY crawled_time -> cal::local_datetime;
      CREATE REQUIRED PROPERTY num -> std::str;
      CREATE REQUIRED PROPERTY read_count -> std::int64;
      CREATE REQUIRED PROPERTY title -> std::str;
      CREATE REQUIRED PROPERTY write_date -> cal::local_date;
      CREATE REQUIRED PROPERTY writer -> std::str;
  };
  CREATE TYPE default::school {
      CREATE REQUIRED PROPERTY files -> array<tuple<name: std::str, url: std::str>>;
      CREATE REQUIRED PROPERTY article_url -> std::str;
      CREATE REQUIRED PROPERTY content -> std::str;
      CREATE REQUIRED PROPERTY crawled_time -> cal::local_datetime;
      CREATE REQUIRED PROPERTY num -> std::str;
      CREATE REQUIRED PROPERTY read_count -> std::int64;
      CREATE REQUIRED PROPERTY title -> std::str;
      CREATE REQUIRED PROPERTY write_date -> cal::local_date;
      CREATE REQUIRED PROPERTY writer -> std::str;
  };
  CREATE TYPE default::sim {
      CREATE REQUIRED PROPERTY files -> array<tuple<name: std::str, url: std::str>>;
      CREATE REQUIRED PROPERTY article_url -> std::str;
      CREATE REQUIRED PROPERTY content -> std::str;
      CREATE REQUIRED PROPERTY crawled_time -> cal::local_datetime;
      CREATE REQUIRED PROPERTY num -> std::str;
      CREATE REQUIRED PROPERTY read_count -> std::int64;
      CREATE REQUIRED PROPERTY title -> std::str;
      CREATE REQUIRED PROPERTY write_date -> cal::local_date;
      CREATE REQUIRED PROPERTY writer -> std::str;
  };
  CREATE FUTURE nonrecursive_access_policies;
};
