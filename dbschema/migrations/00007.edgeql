CREATE MIGRATION m1jyvux74coz2bsjgobtgb5ab5vxlomndpdfb4wyz3xujd7un7azrq
    ONTO m122xc7r2bm5ujpquskhpuynqh6gqw3hxxqusqfrc7n7x26bdho6da
{
  ALTER TYPE default::arch {
      ALTER LINK files {
          SET MULTI;
      };
  };
  ALTER TYPE default::cse {
      ALTER LINK files {
          SET MULTI;
      };
  };
  ALTER TYPE default::dorm {
      ALTER LINK files {
          SET MULTI;
      };
  };
  ALTER TYPE default::emc {
      ALTER LINK files {
          SET MULTI;
      };
  };
  ALTER TYPE default::ide {
      ALTER LINK files {
          SET MULTI;
      };
  };
  ALTER TYPE default::ite {
      ALTER LINK files {
          SET MULTI;
      };
  };
  ALTER TYPE default::mechanical {
      ALTER LINK files {
          SET MULTI;
      };
  };
  ALTER TYPE default::mechatronics {
      ALTER LINK files {
          SET MULTI;
      };
  };
  ALTER TYPE default::school {
      ALTER LINK files {
          SET MULTI;
      };
  };
  ALTER TYPE default::sim {
      ALTER LINK files {
          SET MULTI;
      };
  };
};
