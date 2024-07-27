CREATE MIGRATION m1qyc2lwtbbemjllxtq7zmv7rtqaufrjehkays7tbzjghthvie35ja
    ONTO m1cjsb5el5tbqsgtjtzqenlsl52tzzml4fvauvb564mkldupevftqa
{
  ALTER SCALAR TYPE default::Category EXTENDING enum<NONE, NOTICE, EA, CA, WORK, ETC, DORM_NOTICE, DORM_SAFETY, DORM_RECRUITMENT>;
};
