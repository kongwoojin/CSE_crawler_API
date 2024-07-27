CREATE MIGRATION m13xv4chzzd6qtwaxyzzih7nxsz3moyvtyq33zp5hgm5k3uyiaq3yq
    ONTO m1utdzf5ty2koifnzgzbgfrrk4jdwdgvb3ce6ykktacc3a6muauyua
{
  ALTER TYPE default::arch {
      ALTER PROPERTY article_url {
          CREATE CONSTRAINT std::exclusive;
      };
  };
  ALTER TYPE default::cse {
      ALTER PROPERTY article_url {
          CREATE CONSTRAINT std::exclusive;
      };
  };
  ALTER TYPE default::dorm {
      ALTER PROPERTY article_url {
          CREATE CONSTRAINT std::exclusive;
      };
  };
  ALTER TYPE default::emc {
      ALTER PROPERTY article_url {
          CREATE CONSTRAINT std::exclusive;
      };
  };
  ALTER TYPE default::ide {
      ALTER PROPERTY article_url {
          CREATE CONSTRAINT std::exclusive;
      };
  };
  ALTER TYPE default::ite {
      ALTER PROPERTY article_url {
          CREATE CONSTRAINT std::exclusive;
      };
  };
  ALTER TYPE default::mechanical {
      ALTER PROPERTY article_url {
          CREATE CONSTRAINT std::exclusive;
      };
  };
  ALTER TYPE default::mechatronics {
      ALTER PROPERTY article_url {
          CREATE CONSTRAINT std::exclusive;
      };
  };
  ALTER TYPE default::school {
      ALTER PROPERTY article_url {
          CREATE CONSTRAINT std::exclusive;
      };
  };
  ALTER TYPE default::sim {
      ALTER PROPERTY article_url {
          CREATE CONSTRAINT std::exclusive;
      };
  };
};
