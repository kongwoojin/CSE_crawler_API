CREATE MIGRATION m1ta35icmzuz47wqqkcpnb3ddsjaugrt6rmpmpssb46e4ugem3r55a
    ONTO m1cfe4vgycbpnrdtwzs45vfc6csxqqcnrzfmehhrx75nhzv5eeahga
{
  ALTER TYPE default::arch {
      ALTER LINK files {
          ON TARGET DELETE ALLOW;
      };
  };
  ALTER TYPE default::cse {
      ALTER LINK files {
          ON TARGET DELETE ALLOW;
      };
  };
  ALTER TYPE default::dorm {
      ALTER LINK files {
          ON TARGET DELETE ALLOW;
      };
  };
  ALTER TYPE default::emc {
      ALTER LINK files {
          ON TARGET DELETE ALLOW;
      };
  };
  ALTER TYPE default::ide {
      ALTER LINK files {
          ON TARGET DELETE ALLOW;
      };
  };
  ALTER TYPE default::ite {
      ALTER LINK files {
          ON TARGET DELETE ALLOW;
      };
  };
  ALTER TYPE default::mechanical {
      ALTER LINK files {
          ON TARGET DELETE ALLOW;
      };
  };
  ALTER TYPE default::mechatronics {
      ALTER LINK files {
          ON TARGET DELETE ALLOW;
      };
  };
  ALTER TYPE default::school {
      ALTER LINK files {
          ON TARGET DELETE ALLOW;
      };
  };
  ALTER TYPE default::school {
      CREATE REQUIRED PROPERTY is_importance -> std::bool {
          SET REQUIRED USING (false);
      };
  };
  ALTER TYPE default::sim {
      ALTER LINK files {
          ON TARGET DELETE ALLOW;
      };
  };
};
