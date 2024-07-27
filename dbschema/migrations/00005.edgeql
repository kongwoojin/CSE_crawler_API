CREATE MIGRATION m14or4l34gczaoktqqippvw53uzzo6zcuacvg3ekctj4mzmna7hcoa
    ONTO m1ngk4xopg5d6k4jfbybngm74yxeqqqtetmncpwn4yjggh6w2b2ujq
{
  ALTER TYPE default::cse {
      DROP PROPERTY files;
  };
  ALTER TYPE default::dorm {
      DROP PROPERTY files;
  };
  ALTER TYPE default::emc {
      DROP PROPERTY files;
  };
  ALTER TYPE default::ide {
      DROP PROPERTY files;
  };
  ALTER TYPE default::ite {
      DROP PROPERTY files;
  };
  ALTER TYPE default::mechanical {
      DROP PROPERTY files;
  };
  ALTER TYPE default::mechatronics {
      DROP PROPERTY files;
  };
  ALTER TYPE default::school {
      DROP PROPERTY files;
  };
  ALTER TYPE default::sim {
      DROP PROPERTY files;
  };
  ALTER TYPE default::cse {
      CREATE LINK files -> default::Files;
  };
  ALTER TYPE default::dorm {
      CREATE LINK files -> default::Files;
  };
  ALTER TYPE default::emc {
      CREATE LINK files -> default::Files;
  };
  ALTER TYPE default::ide {
      CREATE LINK files -> default::Files;
  };
  ALTER TYPE default::ite {
      CREATE LINK files -> default::Files;
  };
  ALTER TYPE default::mechanical {
      CREATE LINK files -> default::Files;
  };
  ALTER TYPE default::mechatronics {
      CREATE LINK files -> default::Files;
  };
  ALTER TYPE default::school {
      CREATE LINK files -> default::Files;
  };
  ALTER TYPE default::sim {
      CREATE LINK files -> default::Files;
  };
};
