CREATE MIGRATION m1cjsb5el5tbqsgtjtzqenlsl52tzzml4fvauvb564mkldupevftqa
    ONTO m1bmlqvzb5nv6uaxunhpy4lmrzwfx6wm5x2zpi3acag2iyy7zbdjpq
{
  ALTER TYPE default::notice {
      ALTER PROPERTY num {
          SET TYPE std::int64 USING (<std::int64>.num);
      };
  };
};
