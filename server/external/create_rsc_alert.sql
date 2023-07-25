declare
    vorder_id   NUMBER;
    vpacket_id  NUMBER;
    vkol        NUMBER:=0;
    vID         NUMBER;
    vobj        NUMBER;
    vadr        NUMBER;
    rscserv     NUMBER;
    rscadr      NUMBER;
    vobjname    VARCHAR2(2000):='';
    vadrname    VARCHAR2(2000):='';
    vname 		VARCHAR2(2000):='{}'; --сюда писать номер базовой станции
    vfilial NUMBER;
    vtype NUMBER:= 432; -- тип  оборудования 432 -БСка
    varea NUMBER;
    nCard NUMBER;
begin


    select obj_id, obj_adr, obj_telzone into vobj, vadr, vfilial from M_TTK.rm_object WHERE obj_name = vname AND obj_type = vtype;

    nCard := M_TTK.RSC_CARD_MANAGER.Add_Card(
            xSpecies_ID         => 46,
            xReal_ID            => vobj);

    nCard := M_TTK.RSC_CARD_MANAGER.Add_Card(
            xSpecies_ID         => 8,
            xReal_ID            => vadr);

    SELECT ID INTO varea FROM (
                                  select a.id, ro.OBJ_NAME
                                  from M_TTK.rsc_lst_area a
                                           join M_TTK.list_telzone ltz on a.telzone = ltz.ltz_cod
                                           join M_TTK.rsc_lst_area_type_entry ate on a.id = ate.area_id
                                           RIGHT JOIN M_TTK.RM_OBJECT ro
                                                      ON ro.OBJ_TELZONE = ltz.LTZ_COD
                                  where ate.area_type_id = 106 AND ro.OBJ_TYPE = 432--тип участка = Аварийный БС
                              ) table1 WHERE TABLE1.OBJ_NAME = vname;
    Select max(id)+1 into vID from M_TTK.rsc_order;
    vorder_id := M_TTK.rsc_order_manager.create_order(
            xID             => vID,
            xTelZone_ID     => vfilial,--филиал
            xUser_ID        => 2400,
            xClaim_ID       => -3, -- Аварийный наряд
            xType_ID        => 108, -- 108 АВР БС
            xBegin_Date     => TO_DATE('{}', 'YYYY-MM-DD HH24:MI:SS'),
            xEstimateDate   => sysdate+3,
            xArea_ID        => varea, -- Зона куда кидать INITIATOR
            xGettingType_ID => 1,
            xNotice         => 'id_esm: {}_', -- Примечание
            xSideInfo       => '',
            xSystem_ID      => 1);
    commit;

    vpacket_id := M_TTK.rsc_order_manager.init_service_bind(vorder_id);
    commit;

    select obj_id, obj_adr into vobj, vadr from M_TTK.rm_object WHERE OBJ_NAME = vname AND obj_type= 432;--obj_id = 1475248; --ID базовой станции

    vobjname := M_TTK.rm_pkg.getobjfname(vobj);
    vadrname := M_TTK.rm_doc.get_adrstr(vadr);

    select id into rscserv from M_TTK.rsc_card_common where real_id = vobj and species_id = 46;
    select id into rscadr from M_TTK.rsc_card_common where real_id = vadr and species_id = 8;

    vkol := M_TTK.rsc_order_manager.add_service(
            xDoc_ID       => vorder_id,
            xPacket_ID    => vpacket_id,
            xService_ID   => rscserv,
            xService_Name => vobjname,
            xAddress_ID   => rscadr,
            xAddress_Name => vadrname,
            xSystem_ID    => 1);
    commit;
end;
