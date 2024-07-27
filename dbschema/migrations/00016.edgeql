CREATE MIGRATION m1uukfmgsbazav2bwjokowcpilrpqdpzbi5dxelpnhi3kb5lgptbva
    ONTO m1p6iwnwhnc3hhndr2liyg2dst5epfxhmphcqzmvco73yokbb6du6q
{
  ALTER SCALAR TYPE default::Board EXTENDING enum<NOTICE, FREE, JOB, PDS, LECTURE, BACHELOR, SCHOLAR>;
  ALTER SCALAR TYPE default::Category EXTENDING enum<NONE, NOTICE, EA, CA, WORK, ETC>;
};
