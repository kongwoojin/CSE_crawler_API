CREATE MIGRATION m1cfe4vgycbpnrdtwzs45vfc6csxqqcnrzfmehhrx75nhzv5eeahga
    ONTO m1jyvux74coz2bsjgobtgb5ab5vxlomndpdfb4wyz3xujd7un7azrq
{
  ALTER TYPE default::Files {
      ALTER PROPERTY file_uri {
          CREATE CONSTRAINT std::exclusive;
      };
  };
};
