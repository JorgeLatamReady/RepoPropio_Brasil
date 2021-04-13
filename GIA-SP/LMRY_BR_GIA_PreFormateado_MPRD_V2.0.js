/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_BR_GIA_PreFormateado_MPRD_V2.0.js           ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Jun 18 2018  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search', 'N/log', 'require', 'N/file', "N/config", 'N/runtime', 'N/query', "N/format", "N/record", "N/task", './BR_LIBRERIA_MENSUAL/LMRY_BR_Reportes_LBRY_V2.0.js', "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_libSendingEmailsLBRY_V2.0.js"],

  function(search, log, require, fileModulo, config, runtime, query, format, recordModulo, task, libreria, libFeature) {
    /**
     * Input Data for processing
     *
     * @return Array,Object,Search,File
     *
     * @since 2016.1
     */
    var objContext = runtime.getCurrentScript();
    var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);
    var namereport = 'BR - GIA'
    var ArrCR01 = [];
    var ArrCR05 = [];
    var ArrCR07 = [];
    var ArrCR10 = [];
    var ArrCR14 = [];
    var ArrCR20 = [];
    var ArrCR30 = [];
    var ArrTransactions = [];

    // Parámetros
    var paramPeriod = null;
    var paramSubsi = null;
    var paramMulti = null;
    var paramFeature = null;
    var paramTipoGIA = null;
    var paramTransmitida = null;
    var paramLogID = null;

    var companyname;
    var companyruc;
    var periodname;
    var multibookName;
    var periodStartDate;
    var AcredICMS = false;
    // Features
    var featuresubs = false;
    var featureMulti = false;

    featuresubs = runtime.isFeatureInEffect({
      feature: 'SUBSIDIARIES'
    });
    featureMulti = runtime.isFeatureInEffect({
      feature: 'MULTIBOOK'
    });

    function getInputData() {
      try {
        log.debug('inicia reporte');
        ObtenerParametrosYFeatures();
        ObtenerDatosSubsidiaria();

        var ArrReturn = [];

        ArrCR01 = ObtenerCR01();
        if (AcredICMS) {
          ArrCR05 = ObtenerSaldoAnterior(); //transacciones del mes pasado
        }
        ArrCR07 = ObtenerCR07();

        Facturas = obtenerLineasItemF(paramPeriod);
        Remesas = obtenerLineasItemR(paramPeriod);
        ArrCR10 = Facturas.concat(Remesas);

        ArrCR20 = ObtenerCR20();
        ArrReturn.push(ArrCR01);

        if (ArrCR05.length != 0) {
          for (var i = 0; i < ArrCR05.length; i++) {
            ArrReturn.push(ArrCR05[i]);
          }
        }

        if (ArrCR10.length != 0) {
          for (var i = 0; i < ArrCR10.length; i++) {
            ArrReturn.push(ArrCR10[i]);
          }

          if (ArrCR07.length != 0) {
            for (var i = 0; i < ArrCR07.length; i++) {
              ArrReturn.push(ArrCR07[i]);
            }
          }
        }

        if (ArrCR20.length != 0) {
          for (var i = 0; i < ArrCR20.length; i++) {
            ArrReturn.push(ArrCR20[i]);
          }
        }

        return ArrReturn;
      } catch (err) {
        log.error('err getInputData', err);
        //libreria.sendemailTranslate(LMRY_script, ' [ getInputData ] ' + err);
      }
    }

    /**
     * If this entry point is used, the map function is invoked one time for each key/value.
     *
     * @param {Object} context
     * @param {boolean} context.isRestarted - Indicates whether the current invocation represents a restart
     * @param {number} context.executionNo - Version of the bundle being installed
     * @param {Iterator} context.errors - This param contains a "iterator().each(parameters)" function
     * @param {string} context.key - The key to be processed during the current invocation
     * @param {string} context.value - The value to be processed during the current invocation
     * @param {function} context.write - This data is passed to the reduce stage
     *
     * @since 2016.1
     */
    function map(context) {
      try {
        var arrMap = JSON.parse(context.value);

        paramSubsi = objContext.getParameter({
          name: 'custscript_lmry_gia_preformat_subsi'
        });

        var key = context.key;
        var strReturn10 = '';
        var strReturn14 = '';
        var strReturn30 = '';

        var ArrStr14 = [];

        if (arrMap[0] == '10') {

          var arrayData = [];
          arrayData[0] = arrMap[0];
          arrayData[1] = arrMap[3];

          if (arrMap[2] == 'VendBill' || arrMap[2] == 'CustInvc' || arrMap[2] == 'VendCred' || arrMap[2] == 'CustCred') {
            var TaxResults = ObtenerTaxResults(arrMap[1], arrMap[6], arrMap[8]);
            var jsonMontos = extraerMontosDeclarativos(TaxResults, arrMap);

            arrayData[2] = jsonMontos['valorContable']
            arrayData[3] = jsonMontos['baseCalculo'];
            arrayData[4] = jsonMontos['impuesto'];
            arrayData[5] = jsonMontos['isentas'];
            arrayData[6] = jsonMontos['outras'];
            arrayData[7] = jsonMontos['otrosImpuestos'];
            arrayData[8] = arrMap[4];//UF ID
            arrayData[9] = arrMap[5];//Es contribuyente
            arrayData[10] = arrMap[7];//Municipio ID
          }else{/******************* REMESAS *******************************/
            arrayData[2] = arrMap[6];
            arrayData[3] = 0;
            arrayData[4] = 0;
            arrayData[5] = 0;
            arrayData[6] = arrMap[6];
            arrayData[7] = 0;

            if (arrMap[2] == 'itemfulfillment') {
              var entity = search.lookupFields({
                type: search.Type.SALES_ORDER,
                id: arrMap[4],
                columns: ['billingaddress.custrecord_lmry_addr_prov','billingaddress.custrecord_lmry_addr_city']
              });
            }else{
              var entity = search.lookupFields({
                type: search.Type.PURCHASE_ORDER,
                id: arrMap[4],
                columns: ['billingaddress.custrecord_lmry_addr_prov','billingaddress.custrecord_lmry_addr_city']
              });
            }

            var uf = entity['billingaddress.custrecord_lmry_addr_prov'];
            if (uf != null && uf != '' && uf != '- None -') {
              uf = uf[0].value;
            }else{
              uf = ''
            }
            arrayData[8] = uf;
            arrayData[9] = arrMap[5];

            var muni = entity['billingaddress.custrecord_lmry_addr_city'];
            if (muni != null && muni != '' && muni != '- None -') {
              muni = muni[0].value;
            }else{
              muni = ''
            }
            arrayData[10] = muni;
          }

          if (arrayData[2] != 0 ) {
            //log.debug('arrayData 14',arrayData);
            strReturn10 = formatearlinea(arrayData);

            var inicialCFOP = (arrayData[1]).charAt(0);
            if (inicialCFOP == '2' || inicialCFOP == '6') {
              var Arr14 = armarlineaCR14(arrayData, inicialCFOP);
              strReturn14 = formatearlinea(Arr14);
            }

            var Arr30 = armarlineaCR30(arrayData);
            if (Arr30[2] != '' && Arr30[1] != '') {//ICMS MUNIPIO Y DIPAM DEBE EXISTIR PARA SALIR EN CR30
              strReturn30 = formatearlinea(Arr30);
            }

          }

        } else if (arrMap[0] == '05') {
          var valContable = 0;

          if (arrMap[2] == 'VendBill' || arrMap[2] == 'CustInvc' || arrMap[2] == 'VendCred' || arrMap[2] == 'CustCred') {
            var TaxResults = ObtenerTaxResults(arrMap[1], arrMap[6], arrMap[8]);
            var jsonMontos = extraerMontosDeclarativos(TaxResults, arrMap);
            valContable = jsonMontos['valorContable'];
          }else{/******************* REMESAS *******************************/
            valContable = arrMap[6];
          }

          if (valContable != 0) {
            arrMap[2] = valContable;
            var strReturn05 = formatearlinea(arrMap);
            //log.debug('linea cr05 enviada desde map', strReturn05);
            context.write({
              key: key,
              value: {
                strGIA: strReturn05
              }
            });
          }
        } else { //CR = 07
          strReturn10 = formatearlinea(arrMap);
        }

        context.write({
          key: key,
          value: {
            strGIA: strReturn10
          }
        });

        if (strReturn14 != '') {
          context.write({
            key: key,
            value: {
              strGIA: strReturn14
            }
          });
        }

        if (strReturn30 != '') {
          context.write({
            key: key,
            value: {
              strGIA: strReturn30
            }
          });
        }
      } catch (err) {
        log.error('err map', err);
      }
    }

    /**
     * If this entry point is used, the reduce function is invoked one time for
     * each key and list of values provided..
     *
     * @param {Object} context
     * @param {boolean} context.isRestarted - Indicates whether the current invocation of the represents a restart.
     * @param {number} context.concurrency - The maximum concurrency number when running the map/reduce script.
     * @param {Date} 0context.datecreated - The time and day when the script began running.
     * @param {number} context.seconds - The total number of seconds that elapsed during the processing of the script.
     * @param {number} context.usage - TThe total number of usage units consumed during the processing of the script.
     * @param {number} context.yields - The total number of yields that occurred during the processing of the script.
     * @param {Object} context.inputSummary - Object that contains data about the input stage.
     * @param {Object} context.mapSummary - Object that contains data about the map stage.
     * @param {Object} context.reduceSummary - Object that contains data about the reduce stage.
     * @param {Iterator} context.output - This param contains a "iterator().each(parameters)" function
     *
     * @since 2016.1
     */
    function summarize(context) {
      try {
        ObtenerParametrosYFeatures();
        ObtenerDatosSubsidiaria();

        var strFinal = '';
        var ArrCR01 = [];
        var ArrLineasCR05 = [];
        var ArrCR07 = [];
        var ArrCR05 = [];
        var ArrCR10 = [];
        var ArrCR14 = [];
        var ArrCR20 = [];
        var ArrCR30 = [];

        context.output.iterator().each(function(key, value) {
          var obj = JSON.parse(value);
          var strGIA = obj.strGIA;
          var arrTemp = strGIA.split('|');

          if (arrTemp[0] == '01') {
            ArrCR01.push(arrTemp);
          } else if (arrTemp[0] == '05') {
            ArrLineasCR05.push(arrTemp);
          } else if (arrTemp[0] == '07') {
            ArrCR07.push(arrTemp);
          } else if (arrTemp[0] == '10') {
            ArrCR10.push(arrTemp);
          } else if (arrTemp[0] == '14') {
            ArrCR14.push(arrTemp);
          } else if (arrTemp[0] == '20') {
            ArrCR20.push(arrTemp);
          } else if (arrTemp[0] == '30') {
            ArrCR30.push(arrTemp);
          }

          return true;
        });

        if (ArrCR30.length > 1) {
          ArrCR30 = AgruparPorMunicipio(ArrCR30);
        }
        if (ArrCR10.length > 1) {
          ArrCR10 = AgruparPorCFOP(ArrCR10);
        }
        if (ArrCR14.length > 1) {
          ArrCR14 = AgruparPorCFOPyUF(ArrCR14);
        }
        if (ArrCR20.length > 1) {
          ArrCR20 = acumularCR20(ArrCR20);
        }
        // CR01
        if (ArrCR01.length != 0) {
          for (var i = 0; i < ArrCR01[0].length; i++) {
            strFinal += ArrCR01[0][i];
          }
        }
        strFinal += '\r\n';

        ArrCR05 = ObtenerCR05();

        if (ArrCR05.length != 0) {
          ArrCR05[16] = completar_caracter(4, ArrCR07.length, '0', true);
          ArrCR05[17] = completar_caracter(4, ArrCR10.length, '0', true);
          ArrCR05[18] = completar_caracter(4, ArrCR20.length, '0', true);
          ArrCR05[19] = completar_caracter(4, ArrCR30.length, '0', true);
        }

        if (ArrCR10.length != 0 && ArrCR05.length != 0) {
          ArrCR05[8] = 1;
        }

        /* ACUMULAR SALDO ANTERIOR (VALOR CONTABLE) PARA CR05*/
        var valorContableCR05 = 0;
        for (var i = 0; i < ArrLineasCR05.length; i++) {
          valorContableCR05 += Number(ArrLineasCR05[i][2]);
        }
        ArrCR05[10] = completar_caracter(15, ((redondear(valorContableCR05)).toFixed(2)).replace('.', ''), '0', true);

        // CR05
        if (ArrCR05.length != 0) {
          for (var i = 0; i < ArrCR05.length; i++) {
            strFinal += ArrCR05[i];
          }
        }
        strFinal += '\r\n';

        // CR07
        if (ArrCR07.length != 0) {
          for (var i = 0; i < ArrCR07.length; i++) {
            var CR = '07';
            var PropriaOuST = '0';
            var Imposto = completar_caracter(15, ((redondear(ArrCR07[i][2])).toFixed(2)).replace('.', ''), '0', true);
            var Date_t = ArrCR07[i][3];

            strFinal += CR + PropriaOuST + Imposto + Date_t;
            strFinal += '\r\n';
          }
        }

        var ArrCR10y14 = [];

        // Actualizar los Q14 del CR10
        if (ArrCR14.length != 0) {
          var ArrTemporal = [];

          var cont = 0;

          for (var i = 0; i < ArrCR10.length; i++) {
            var cantidad_14 = 0;
            ArrTemporal[cont] = ArrCR10[i];
            var cont_temporal = cont;
            cont++;

            for (var j = 0; j < ArrCR14.length; j++) {
              if (ArrCR14[j][13] == ArrCR10[i][1]) {
                cantidad_14++;
                ArrTemporal[cont] = ArrCR14[j];
                cont++;
              }
            }

            if (cantidad_14 != 0) {
              ArrTemporal[cont_temporal][11] = cantidad_14;
            }
          }

          ArrCR10y14 = ArrTemporal;
        } else {
          ArrCR10y14 = ArrCR10;
        }

        for (var i = 0; i < ArrCR10y14.length; i++) {
          if (ArrCR10y14[i][0] == '10') {
            var CR = '10';
            var CFOP = completar_caracter(6, ArrCR10y14[i][1].replace('.', ''), '0', false);
            var ValorContábil = completar_caracter(15, ((redondear(ArrCR10y14[i][2])).toFixed(2)).replace('.', ''), '0', true);
            var BaseCálculo = completar_caracter(15, ((redondear(ArrCR10y14[i][3])).toFixed(2)).replace('.', ''), '0', true);
            var Imposto = completar_caracter(15, ((redondear(ArrCR10y14[i][4])).toFixed(2)).replace('.', ''), '0', true);
            var IsentasNãoTrib = completar_caracter(15, ((redondear(ArrCR10y14[i][5])).toFixed(2)).replace('.', ''), '0', true);
            var Outras = completar_caracter(15, ((redondear(ArrCR10y14[i][6])).toFixed(2)).replace('.', ''), '0', true);
            var ImpostoRetidoST = completar_caracter(15, ((redondear(0)).toFixed(2)).replace('.', ''), '0', true);
            var ImpRetSubstitutoST = completar_caracter(15, ((redondear(0)).toFixed(2)).replace('.', ''), '0', true);
            var ImpRetSubstituído = completar_caracter(15, ((redondear(0)).toFixed(2)).replace('.', ''), '0', true);
            var OutrosImpostos = completar_caracter(15, ((redondear(ArrCR10y14[i][7])).toFixed(2)).replace('.', ''), '0', true);
            var Q14 = completar_caracter(4, ArrCR10y14[i][11], '0', true);

            strFinal += CR + CFOP + ValorContábil + BaseCálculo + Imposto + IsentasNãoTrib + Outras + ImpostoRetidoST + ImpRetSubstitutoST + ImpRetSubstituído + OutrosImpostos + Q14;
            strFinal += '\r\n';
          } else {
            var CR = '14';
            var UF = ArrCR10y14[i][1];
            var Valor_Contábil_1 = completar_caracter(15, ((redondear(ArrCR10y14[i][2])).toFixed(2)).replace('.', ''), '0', true);
            var BaseCalculo_1 = completar_caracter(15, ((redondear(ArrCR10y14[i][3])).toFixed(2)).replace('.', ''), '0', true);
            var Valor_Contábil_2 = completar_caracter(15, ((redondear(ArrCR10y14[i][4])).toFixed(2)).replace('.', ''), '0', true);
            var BaseCalculo_2 = completar_caracter(15, ((redondear(ArrCR10y14[i][5])).toFixed(2)).replace('.', ''), '0', true);
            var Imposto = completar_caracter(15, ((redondear(ArrCR10y14[i][6])).toFixed(2)).replace('.', ''), '0', true);
            var Outras = completar_caracter(15, ((redondear(ArrCR10y14[i][7])).toFixed(2)).replace('.', ''), '0', true);
            var ICMSCobradoST = completar_caracter(15, ((redondear(0)).toFixed(2)).replace('.', ''), '0', true);
            var PetróleoEnergia = completar_caracter(15, ((redondear(0)).toFixed(2)).replace('.', ''), '0', true);
            var Outros_Produtos = completar_caracter(15, ((redondear(0)).toFixed(2)).replace('.', ''), '0', true);
            var Benef = 0;
            var Q18 = completar_caracter(4, ((redondear(0)).toFixed(2)).replace('.', ''), '0', true);

            strFinal += CR + UF + Valor_Contábil_1 + BaseCalculo_1 + Valor_Contábil_2 + BaseCalculo_2 + Imposto + Outras + ICMSCobradoST + PetróleoEnergia + Outros_Produtos + Benef + Q18;
            strFinal += '\r\n';
          }
        }

        if (ArrCR20.length != 0) {
          for (var i = 0; i < ArrCR20.length; i++) {
            var CR = ArrCR20[i][0];
            var CodSubItem = ArrCR20[i][1];
            var Valor = completar_caracter(15, ((redondear(ArrCR20[i][2])).toFixed(2)).replace('.', ''), '0', true);
            var PropriaOuST = ArrCR20[i][3];
            var FLegal = completar_caracter(100, ArrCR20[i][4], ' ', true);
            var Ocurrencia = completar_caracter(300, ArrCR20[i][5], ' ', true);
            var Q25 = completar_caracter(4, '0', '0', true);
            var Q26 = completar_caracter(4, '0', '0', true);
            var Q27 = completar_caracter(4, '0', '0', true);
            var Q28 = completar_caracter(4, '0', '0', true);

            strFinal += CR + CodSubItem + Valor + PropriaOuST + FLegal + Ocurrencia + Q25 + Q26 + Q27 + Q28;
            strFinal += '\r\n';
          }
        }

        if (ArrCR30.length != 0) {
          for (var i = 0; i < ArrCR30.length; i++) {
            var CR = ArrCR30[i][0];
            var CodDip = ArrCR30[i][1];
            var Municipio = ArrCR30[i][2];
            var Valor = completar_caracter(15, ((redondear(ArrCR30[i][3])).toFixed(2)).replace('.', ''), '0', true);

            strFinal += CR + CodDip + Municipio + Valor;
            strFinal += '\r\n';
          }
        }

        if (strFinal != '') {
          saveFile(strFinal);
        } else {
          NoData();
        }
      } catch (err) {
        log.error('err summarize', err);
        //libreria.sendemailTranslate(LMRY_script, ' [ getInputData ] ' + err);
      }
    }

    function obtenerLineasItemR(periodo){
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = [];

      var savedsearch = search.load({
        /*LatamReady - BR GIA Remesas*/
        id: 'customsearch_lmry_br_gia_rem'
      });

      if (featuresubs) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var periodFilter = search.createFilter({
        name: 'postingperiod',
        operator: search.Operator.IS,
        values: [periodo]
      });
      savedsearch.filters.push(periodFilter);

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arr = new Array();
            // 0. CR
            if (periodo == paramPeriod) {
              arr[0] = '10';
            } else {
              arr[0] = '05';
            }
            // 1. Internal ID
            arr[1] = objResult[i].getValue(columns[0]);
            // 2. Tipo Transaccion
            arr[2] = objResult[i].getValue(columns[5]);
            // 3. CFOP
            arr[3] = objResult[i].getValue(columns[1]);
            // 4. ID PO/SO
            arr[4] = objResult[i].getValue(columns[3]);
            // 5. Contribuyente
            arr[5] = objResult[i].getValue(columns[4]);
            // 6. Valor Contable
            arr[6] = redondear(objResult[i].getValue(columns[2]));

            ArrReturn.push(arr);
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }

        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    function formatearlinea(arrData){
      var strlinea = '';
      for (var i = 0; i < arrData.length; i++) {
        strlinea += arrData[i];
        if (i != arrData.length - 1) {
          strlinea += '|'
        }
      }
      return strlinea;
    }

    function armarlineaCR14(arrData, inicialCFOP){
      var arr14 = [];
      // 0. CR
      arr14[0] = '14';
      // 1. UF
      var ufEntity = search.lookupFields({
        type: 'customrecord_lmry_province',
        id: arrData[8],
        columns: ['custrecord_lmry_prov_id_alternative']
      });

      var codUF = ufEntity.custrecord_lmry_prov_id_alternative;
      if (codUF != '') {
        arr14[1] = completar_caracter(2, codUF, ' ', true);
      }else{
        arr14[1] = completar_caracter(2, '', ' ', true);
      }

      var Valor_Contábil_1 = 0;
      var Valor_Contábil_2 = 0;
      var BaseCalculo_1 = 0;
      var BaseCalculo_2 = 0;

      if (inicialCFOP == '2') {
        Valor_Contábil_1 = arrData[2];
        BaseCalculo_1 = arrData[3];
      } else {
        if (arrData[5] == 3) {
          Valor_Contábil_2 = arrData[2];
          BaseCalculo_2 = arrData[3];
        } else {
          Valor_Contábil_1 = arrData[2];
          BaseCalculo_1 = arrData[3];
        }
      }

      arr14[2] = Valor_Contábil_1;
      arr14[3] = BaseCalculo_1;
      arr14[4] = Valor_Contábil_2;
      arr14[5] = BaseCalculo_2;
      //6. Impuesto
      arr14[6] = arrData[4];
      //7. Outras
      arr14[7] = arrData[6];
      // 8. ICMSCobradoST
      arr14[8] = 0;
      // 9. PetróleoEnergia
      arr14[9] = 0;
      //10. Outros_Produtos
      arr14[10] = 0;
      // 11. Benef
      arr14[11] = 0;
      // 12. Q18
      arr14[12] = 0;
      // 13. CFOP
      arr14[13] = arrData[1];

      return arr14;
    }

    function armarlineaCR30(arrData){
      var arr30 = [];
      // 0. CR
      arr30[0] = '30';
      // 1. CodDip
      var subsiLookUp = search.lookupFields({
        type: search.Type.SUBSIDIARY,
        id: paramSubsi,
        columns: ['custrecord_lmry_br_dipamb']
      });

      var codDipamB = '';
      if (subsiLookUp.custrecord_lmry_br_dipamb.length != 0) {
        var DipamCodeLookUp = search.lookupFields({
          type: 'customrecord_lmry_br_dipam',
          id: (subsiLookUp.custrecord_lmry_br_dipamb[0]).value,
          columns: ['custrecord_lmry_br_coddipam']
        });
        codDipamB = (DipamCodeLookUp.custrecord_lmry_br_coddipam).replace('.', '');
      }
      arr30[1] = codDipamB;
      // 2. Municipio
      arr30[2] = '';
      var Municipio_address_search_id = arrData[10];

      var CityLookUp = search.lookupFields({
        type: 'customrecord_lmry_city',
        id: Municipio_address_search_id,
        columns: ['custrecord_lmry_idcity_icms']
      });

      if (CityLookUp.custrecord_lmry_idcity_icms != '') {
        arr30[2] = completar_caracter(5, CityLookUp.custrecord_lmry_idcity_icms, '0', true);
      }
      // 3. Valor Contable
      arr30[3] = arrData[2];

      return arr30;
    }

    function extraerMontosDeclarativos(taxResults, lineaItem) {
      var baseCalculo = 0;
      var impuesto = 0;
      var isentas = 0;
      var outras = 0;
      var otros_Impuestos = 0;

      var seUsaCST_COL = false;
      if (lineaItem[2] == 'CustInvc' || lineaItem[2] == 'CustCred') {
        seUsaCST_COL = true;
      }

      for (var i = 0; i < taxResults.length; i++) {

        var cst = taxResults[i][4]; //caso general CST sale del tax result
        var porcImp = taxResults[i][2];

        if (taxResults[i][3] == 'ICMS') {

          if (porcImp == 0) {

            if (seUsaCST_COL) { //si es ventas y alicuota es 0, CST sale de transaction
              if (lineaItem[9] != '' && lineaItem[9] != null) {
                cst = obtenerCST(lineaItem[9]);
              } else {
                cst = '';
              }
            }
            if (cst == '090') { //OUTRAS
              outras = Number(outras) + Number(taxResults[i][0]);
            }
            if (cst == '040' || cst == '041') { //ISENTAS / NAO TRIBUT.
              isentas = Number(isentas) + Number(taxResults[i][0]);
            }

          } else {

            if (cst != '090' && cst != '040' && cst != '041' && cst != '' && cst != null) {
              baseCalculo = Number(baseCalculo) + Number(taxResults[i][0]);
              impuesto = Number(impuesto) + Number(taxResults[i][1]);
            }
          }

        } else { //OTROS IMPUESTOS

          if (taxResults[i][3] == 'IPI') {
            if (porcImp != 0) {
              /* Outras - Isentas - Notribut */
              if (cst != '02' && cst != '03' && cst != '49' && cst != '' && cst != null) {
                otros_Impuestos = Number(otros_Impuestos) + Number(taxResults[i][1]);
              }
            }
          }

        }

      }
      //VC = BC + isentas + outras + ImpostoRetidoST + Otros impuestos (segun validador)
      var valorContable = baseCalculo + isentas + outras + otros_Impuestos;

      var jsonMontos = {
        'baseCalculo': baseCalculo,
        'impuesto': impuesto,
        'isentas': isentas,
        'outras': outras,
        'otrosImpuestos': otros_Impuestos,
        'valorContable': valorContable
      }

      return jsonMontos;
    }

    function obtenerCST(id) {
      var cstRecord = search.lookupFields({
        type: 'customrecord_lmry_br_tax_situation',
        id: id,
        columns: ['custrecord_lmry_br_tax_situacion_code']
      });
      return cstRecord.custrecord_lmry_br_tax_situacion_code;
    }

    function AgruparPorMunicipio(ArrData) {
      // 0. CR
      // 1. CodDip
      // 2. Municipio
      // 3. Valor
      var ArrReturn = [];
      ArrData.sort(sortFunction);

      function sortFunction(a, b) {
        if (a[2] === b[2]) {
          return 0;
        } else {
          return (a[2] < b[2]) ? -1 : 1;
        }
      }

      var pivote = ArrData[0];

      for (var i = 1; i < ArrData.length; i++) {
        if (pivote[2] == ArrData[i][2]) {
          pivote[3] = Number(pivote[3]) + Number(ArrData[i][3]);
        } else {
          ArrReturn.push(pivote);

          pivote = ArrData[i];
        }

        if (i == ArrData.length - 1) {
          ArrReturn.push(pivote);
        }
      }

      return ArrReturn;
    }

    function AgruparPorCFOPyUF(ArrData) {
      var ArrReturn = [];

      // Se ordena por CFOP y UF
      ArrData.sort(function(a, b) {
        var aCFOP = a[13];
        var bCFOP = b[13];
        var aUF = a[1];
        var bUF = b[1];

        if (aCFOP == bCFOP) {
          return (aUF < bUF) ? -1 : (aUF > bUF) ? 1 : 0;
        } else {
          return (aCFOP < bCFOP) ? -1 : 1;
        }
      });

      var pivote = ArrData[0];

      for (var i = 1; i < ArrData.length; i++) {
        if (pivote[13] == ArrData[i][13]) {
          if (pivote[1] == ArrData[i][1]) {
            pivote[2] = Number(pivote[2]) + Number(ArrData[i][2]);
            pivote[3] = Number(pivote[3]) + Number(ArrData[i][3]);
            pivote[4] = Number(pivote[4]) + Number(ArrData[i][4]);
            pivote[5] = Number(pivote[5]) + Number(ArrData[i][5]);
            pivote[6] = Number(pivote[6]) + Number(ArrData[i][6]);
            pivote[7] = Number(pivote[7]) + Number(ArrData[i][7]);
            pivote[8] = Number(pivote[8]) + Number(ArrData[i][8]);
            pivote[9] = Number(pivote[9]) + Number(ArrData[i][9]);
            pivote[10] = Number(pivote[10]) + Number(ArrData[i][10]);
            pivote[11] = Number(pivote[11]) + Number(ArrData[i][11]);
            pivote[12] = Number(pivote[12]) + Number(ArrData[i][12]);
          } else {
            ArrReturn.push(pivote);

            pivote = ArrData[i];
          }
        } else {
          ArrReturn.push(pivote);

          pivote = ArrData[i];
        }

        if (i == ArrData.length - 1) {
          ArrReturn.push(pivote);
        }
      }

      return ArrReturn;
    }

    function AgruparPorCFOP(ArrData) {
      var ArrReturn = [];

      ArrData.sort(sortFunction);

      function sortFunction(a, b) {
        if (a[1] === b[1]) {
          return 0;
        } else {
          return (a[1] < b[1]) ? -1 : 1;
        }
      }

      var pivote = ArrData[0];

      for (var i = 1; i < ArrData.length; i++) {
        if (pivote[1] == ArrData[i][1]) {
          pivote[2] = Number(pivote[2]) + Number(ArrData[i][2]);
          pivote[3] = Number(pivote[3]) + Number(ArrData[i][3]);
          pivote[4] = Number(pivote[4]) + Number(ArrData[i][4]);
          pivote[5] = Number(pivote[5]) + Number(ArrData[i][5]);
          pivote[6] = Number(pivote[6]) + Number(ArrData[i][6]);
          pivote[7] = Number(pivote[7]) + Number(ArrData[i][7]);
        } else {
          ArrReturn.push(pivote);

          pivote = ArrData[i];
        }

        if (i == ArrData.length - 1) {
          ArrReturn.push(pivote);
        }
      }

      return ArrReturn;
    }

    function acumularCR20(arrayData) {
      var arrActualizado = [];
      var long = arrayData.length;
      var i = 0;
      var j = 0;

      while (j < long) {
        var monto = Number(arrayData[j][2]);
        i = j + 1;
        while (i < long) {
          if (arrayData[j][1] == arrayData[i][1]) {
            monto += Number(arrayData[i][2]);
            arrayData.splice(i, 1);
            long--;
          } else {
            i++;
          }
        }
        arrayData[j][2] = monto;
        arrActualizado.push(arrayData[j]);
        j++;
      }
      //log.debug('arrActualizado cr20', arrActualizado);
      return arrActualizado;
    }

    function ObtenerTaxResults(TransactionID, LineUniqueKey, exchangeRate) {
      var DbolStop = false;
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var ArrReturn = [];

      var savedsearch = search.create({
        type: "customrecord_lmry_br_transaction",
        filters: [
          ["custrecord_lmry_br_transaction", "is", TransactionID],
          "AND",
          ["custrecord_lmry_lineuniquekey", "equalto", LineUniqueKey]
        ],
        columns: [
          search.createColumn({
            name: "formulanumeric",
            formula: "{custrecord_lmry_base_amount}",
            label: "0. Base Amount"
          }),
          search.createColumn({
            name: "formulanumeric",
            formula: "{custrecord_lmry_br_total}",
            label: "1. Imposto"
          }),
          search.createColumn({
            name: "formulanumeric",
            formula: "{custrecord_lmry_br_percent}",
            label: "2. Percentage"
          }),
          search.createColumn({
            name: "formulatext",
            formula: "{custrecord_lmry_br_type}",
            label: "3. Tribute ID"
          }),
          search.createColumn({
            name: "formulatext",
            formula: "{custrecord_lmry_br_tax_taxsituation_cod}",
            label: "4. Tax Situacion Code"
          }),
          search.createColumn({
            name: "formulanumeric",
            formula: "{custrecord_lmry_base_amount_local_currc}",
            label: "5. Base Amount Local Currency"
          }),
          search.createColumn({
            name: "formulanumeric",
            formula: "{custrecord_lmry_amount_local_currency}",
            label: "6. Impuesto Local Currency"
          })
        ]
      });

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arr = new Array();

            // 0. Base Amount
            var montoBase = objResult[i].getValue(columns[5]);
            if (montoBase != null && montoBase != 0 && montoBase != "- None -") {
              arr[0] = redondear(montoBase);
            } else {
              arr[0] = redondear(objResult[i].getValue(columns[0])) * exchangeRate;
              arr[0] = redondear(arr[0]);

            }
            // 1. Imposto
            var impuesto = objResult[i].getValue(columns[6]);
            if (impuesto != null && impuesto != 0 && impuesto != "- None -") {
              arr[1] = redondear(impuesto);
            } else {
              arr[1] = redondear(objResult[i].getValue(columns[1])) * exchangeRate;
              arr[1] = redondear(arr[1]);
            }

            // 2. Percent
            arr[2] = objResult[i].getValue(columns[2]);
            // 3. Tribute ID
            arr[3] = objResult[i].getValue(columns[3]);
            // 4. Codigo Situacion Tributaria
            arr[4] = objResult[i].getValue(columns[4]);

            ArrReturn.push(arr);
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }

        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    function redondear(number) {
      return Math.round(Number(number) * 100) / 100;
    }

    function NoData() {
      var usuarioTemp = runtime.getCurrentUser();
      var id = usuarioTemp.id;

      var employeename = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: id,
        columns: ['firstname', 'lastname']
      });

      var usuario = employeename.firstname + ' ' + employeename.lastname;

      var record = recordModulo.load({
        type: 'customrecord_lmry_br_rpt_generator_log',
        id: paramLogID
      });

      //Nombre de Archivo
      record.setValue({
        fieldId: 'custrecord_lmry_br_rg_name_field',
        value: 'No existe informacion para los criterios seleccionados.'
      });
      //Periodo
      record.setValue({
        fieldId: 'custrecord_lmry_br_rg_period',
        value: periodname
      });
      //Creado Por
      record.setValue({
        fieldId: 'custrecord_lmry_br_rg_employee',
        value: usuario
      });

      var recordId = record.save();
    }

    function ObtenerCR01() {
      var ArrReturn = [];
      // 0. CR
      var CR = '01';
      ArrReturn.push(CR);
      // 1. TipoDocto
      var TipoDocto = '01';
      ArrReturn.push(TipoDocto);

      var DataGeração;

      var today = ObtenerCurrentDate();
      today = today[0][0];

      var today = format.parse({
        value: today,
        type: format.Type.DATE
      });

      var month;

      if (('' + (today.getMonth() + 1)).length == 1) {
        month = '0' + (today.getMonth() + 1);
      } else {
        month = today.getMonth() + 1;
      }

      var day;

      if (('' + today.getDate()).length == 1) {
        day = '0' + today.getDate();
      } else {
        day = today.getDate();
      }

      // 2. DataGeração
      DataGeração = '' + today.getFullYear() + month + day;
      ArrReturn.push(DataGeração);
      // 3. HoraGeração
      var HoraGeração;

      var hours;

      if (('' + today.getHours()).length == 1) {
        hours = '0' + today.getHours();
      } else {
        hours = today.getHours();
      }

      var minutes;

      if (('' + today.getMinutes()).length == 1) {
        minutes = '0' + today.getMinutes();
      } else {
        minutes = today.getMinutes();
      }

      var seconds;

      if (('' + today.getSeconds()).length == 1) {
        seconds = '0' + today.getSeconds();
      } else {
        seconds = today.getSeconds();
      }

      HoraGeração = '' + hours + minutes + seconds;
      ArrReturn.push(HoraGeração);

      // 4. VersãoFrontEnd
      var VersãoFrontEnd = '0000';
      ArrReturn.push(VersãoFrontEnd);

      var featureLook = search.lookupFields({
        type: 'customrecord_lmry_br_features',
        id: paramFeature,
        columns: ['custrecord_lmry_br_version']
      });

      // 5. VersãoPref
      var VersãoPref;
      VersãoPref = featureLook.custrecord_lmry_br_version;
      ArrReturn.push(completar_caracter(4, VersãoPref, '0', true));

      // 6. Q5
      var Q5 = '0001';
      ArrReturn.push(Q5);

      return ArrReturn;
    }

    function ObtenerCurrentDate() {
      var ArrReturn = [];

      var customrecord_lmry_br_featuresSearchObj = search.create({
        type: "customrecord_lmry_br_features",
        columns: [
          search.createColumn({
            name: "formuladatetime",
            formula: "CURRENT_DATE",
            label: "Formula (Date/Time)"
          })
        ]
      });

      var searchresult = customrecord_lmry_br_featuresSearchObj.run();
      var objResult = searchresult.getRange(0, 1);

      if (objResult != null) {

        if (objResult.length != 1000) {
          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arr = new Array();
            // 0. DATE TIME
            arr[0] = objResult[i].getValue(columns[0]);

            ArrReturn.push(arr);
          }
        }
      }

      return ArrReturn;
    }

    function ObtenerCR05() {
      var ArrReturn = [];
      // 0. CR
      var CR = '05';
      ArrReturn.push(CR);
      // 1. IE
      var IE;
      // 2.CNPJ
      var CNPJ;
      // 3. CNAE
      var CNAE;
      // 4. RegTrib
      var RegTrib;

      var subsidiaryRcd = recordModulo.load({
        type: search.Type.SUBSIDIARY,
        id: paramSubsi
      });

      var custrecord_lmry_br_state_tax_sub = subsidiaryRcd.getValue({
        fieldId: 'custrecord_lmry_br_state_tax_sub'
      });

      var federalidnumber = subsidiaryRcd.getValue({
        fieldId: 'federalidnumber'
      });

      var custrecord_lmry_br_regtrib_estatual = subsidiaryRcd.getValue({
        fieldId: 'custrecord_lmry_br_regtrib_estatual'
      });

      IE = completar_caracter(12, ('' + custrecord_lmry_br_state_tax_sub).replace(/\./g, ''), '0', true);
      ArrReturn.push(IE);

      federalidnumber = validarNumeros(federalidnumber);
      CNPJ = completar_caracter(14, ('' + federalidnumber).replace(/\./g, ''), '0', true);
      ArrReturn.push(CNPJ);

      CNAE = '0000000';
      ArrReturn.push(CNAE);

      RegTrib = completar_caracter(2, ('' + custrecord_lmry_br_regtrib_estatual).replace(/\./g, ''), '0', true);
      ArrReturn.push(RegTrib);

      // 5. Ref
      var Ref;
      //agregando variables para el special accounting period

      var licenses = libFeature.getLicenses(paramSubsi);
      var feature_pruebita = libFeature.getAuthorization(599, licenses);
      log.error('feature_pruebita',feature_pruebita);

      if(feature_pruebita){
        var SearchPeriodSpecial = search.create({
          type: "customrecord_lmry_special_accountperiod",
          filters:
          [
             ["isinactive","is","F"],
             "AND",
             ["custrecord_lmry_accounting_period","anyof",paramPeriod]
          ],
          columns:
          [
             search.createColumn({
                name: "formulatext",
                formula: "TO_CHAR({custrecord_lmry_date_ini},'YYYYMM')",
                label: "Formula (Text)"
             })
          ]
       });

       var searchResult = SearchPeriodSpecial.run().getRange(0,100);
          if (searchResult.length != 0) {
                var columns = searchResult[0].columns;
                periodStartDate =  searchResult[0].getValue(columns[0]);
                log.debug('periodStartDate',periodStartDate);
                Ref = '' + periodStartDate;
          }else {
            var periodLook = search.lookupFields({
            type: search.Type.ACCOUNTING_PERIOD,
            id: paramPeriod,
            columns: ['startdate']
            });
            periodStartDate = periodLook.startdate;
            periodStartDate = format.parse({
              value: periodStartDate,
              type: format.Type.DATE
            });

            var refMonth;

            if (('' + (periodStartDate.getMonth() + 1)).length == 1) {
              refMonth = '0' + (periodStartDate.getMonth() + 1);
            } else {
              refMonth = periodStartDate.getMonth() + 1;
            }

            Ref = '' + periodStartDate.getFullYear() + refMonth;
          }
    }else{
        var periodLook = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramPeriod,
        columns: ['startdate']
        });
        periodStartDate = periodLook.startdate;
        periodStartDate = format.parse({
          value: periodStartDate,
          type: format.Type.DATE
        });

        var refMonth;

        if (('' + (periodStartDate.getMonth() + 1)).length == 1) {
          refMonth = '0' + (periodStartDate.getMonth() + 1);
        } else {
          refMonth = periodStartDate.getMonth() + 1;
        }

        Ref = '' + periodStartDate.getFullYear() + refMonth;
      }

      ArrReturn.push(Ref);
      // 6. RefInicial
      var RefInicial;
      if (RegTrib == '01') {
        RefInicial = '000000';
      } else {
        //por ahora
        RefInicial = '000000';
      }
      ArrReturn.push(RefInicial);
      // 7. Tipo
      var Tipo = ('' + completar_caracter(2, paramTipoGIA, '0', true)).replace('.', '');
      ArrReturn.push(Tipo);
      // 8. Movimento
      var Movimento = 0;
      ArrReturn.push(Movimento);
      // 9. Transmitida
      var Transmitida = paramTransmitida;
      ArrReturn.push(Transmitida);
      // 10. SaldoCredPeriodoAnt
      var SaldoCredPeriodoAnt = '';
      ArrReturn.push(SaldoCredPeriodoAnt);
      // 11. SaldoCredPeriodoAntST
      var SaldoCredPeriodoAntST = '000000000000000';
      ArrReturn.push(SaldoCredPeriodoAntST);
      // 12. OrigemSoftware
      var OrigemSoftware = CNPJ;
      ArrReturn.push(OrigemSoftware);
      // 13. OrigemPreDig
      var OrigemPreDig = '0';
      ArrReturn.push(OrigemPreDig);
      // 14. ICMSFixPer
      var ICMSFixPer = '000000000000000';
      ArrReturn.push(ICMSFixPer);
      // 15. ChaveInterna
      var ChaveInterna = '00000000000000000000000000000000';
      ArrReturn.push(ChaveInterna);
      // 16. Q7
      var Q7 = '0000';
      ArrReturn.push(Q7);
      // 17. Q10
      var Q10 = 0;
      ArrReturn.push(Q10);
      // 18. Q20
      var Q20 = '0000';
      ArrReturn.push(Q20);
      // 19. Q30
      var Q30 = '0000';
      ArrReturn.push(Q30);
      // 20. Q31
      var Q31 = '0000';
      ArrReturn.push(Q31);

      return ArrReturn;
    }

    function obtenerPeriodoAnterior() {
      var idPeriodoAnterior = '';

      var accountingperiodSearchObj = search.create({
        type: "accountingperiod",
        filters: [
          ["isadjust", "is", "F"],
          "AND",
          ["isquarter", "is", "F"],
          "AND",
          ["isinactive", "is", "F"],
          "AND",
          ["isyear", "is", "F"]
        ],
        columns: [
          search.createColumn({
            name: "internalid",
            label: "Internal ID"
          }),
          search.createColumn({
            name: "periodname",
            label: "Name"
          }),
          search.createColumn({
            name: "startdate",
            sort: search.Sort.ASC,
            label: "Start Date"
          }),
          search.createColumn({
            name: "enddate",
            label: "End Date"
          }),
          search.createColumn({
            name: "formulatext",
            formula: "TO_CHAR({startdate},'yyyy')",
            label: "Formula (Text)"
          })
        ]
      });

      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;

      var searchresult = accountingperiodSearchObj.run();

      var arrPeriodos = [];
      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;

            if (paramPeriod == objResult[i].getValue(columns[0])) {
              if (i != 0) {
                idPeriodoAnterior = objResult[i - 1].getValue(columns[0]);
                break;
              } else {
                log.debug('Alerta en busqueda periodo anterior', 'No existe periodo anterior al parametro dado para el reporte.');
              }
            }

          }

          if (idPeriodoAnterior != '') {
            break;
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }
        } else {
          DbolStop = true;
        }
      }
      return idPeriodoAnterior;
    }

    function ObtenerSaldoAnterior() {
      var idPeriodAnt = obtenerPeriodoAnterior();
      var arrayPeriodoAnt = [];
      if (idPeriodAnt != '' && idPeriodAnt != null) {
        var lineasFactura = obtenerLineasItemF(idPeriodAnt);
        var lineasRemesa = obtenerLineasItemR(idPeriodAnt);

        arrayPeriodoAnt = lineasFactura.concat(lineasRemesa);
      }
      return arrayPeriodoAnt;
    }

    function validarNumeros(s) {
      s = s.replace(/\./g, '');
      s = s.replace(/\,/g, '');
      s = s.replace(/\_/g, '');
      s = s.replace(/\-/g, '');
      s = s.replace(/\//g, '');
      return s;
    }

    function ObtenerCR07() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = [];

      var savedsearch = search.load({
        id: 'customsearch_lmry_br_gia_cr07'
      });

      if (featuresubs) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var periodFilter = search.createFilter({
        name: 'postingperiod',
        operator: search.Operator.IS,
        values: [paramPeriod]
      });
      savedsearch.filters.push(periodFilter);

      if (featureMulti || featureMulti == 'T') {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [paramMulti]
        });
        savedsearch.filters.push(multibookFilter);

        var amountMulti = search.createColumn({
          name: 'amount',
          join: 'accountingtransaction',
          summary: 'SUM'
        });
        savedsearch.columns.push(amountMulti);
      }

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arr = new Array();
            // 0. CR
            arr[0] = '07';
            // 1. PropriaOutST
            arr[1] = '0';
            // 2. Imposto
            if (featureMulti) {
              arr[2] = objResult[i].getValue(columns[2]);
            } else {
              arr[2] = objResult[i].getValue(columns[0]);
            }
            // 3. Date
            arr[3] = objResult[i].getValue(columns[1]);

            ArrReturn.push(arr);
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }

        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    function obtenerLineasItemF(periodo) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = [];

      var savedsearch = search.load({
        /*LatamReady - BR GIA CR10*/
        id: 'customsearch_lmry_br_gia_cr10'
      });

      if (featuresubs) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var periodFilter = search.createFilter({
        name: 'postingperiod',
        operator: search.Operator.IS,
        values: [periodo]
      });
      savedsearch.filters.push(periodFilter);

      if (featureMulti || featureMulti == 'T') {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [paramMulti]
        });
        savedsearch.filters.push(multibookFilter);
        //10 si hay multi
        var TCMulti = search.createColumn({
          name: 'exchangerate',
          join: 'accountingtransaction'
        });
        savedsearch.columns.push(TCMulti);
      }

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arr = new Array();
            // 0. CR
            if (periodo == paramPeriod) {
              arr[0] = '10';
            } else {
              arr[0] = '05';
            }
            // 1. Internal ID
            arr[1] = objResult[i].getValue(columns[0]);
            // 2. Type
            arr[2] = objResult[i].getValue(columns[9]);
            // 3. CFOP
            arr[3] = objResult[i].getValue(columns[1]);
            // 4. UF ID
            arr[4] = objResult[i].getValue(columns[3]);
            // 5. Contribuyente
            arr[5] = objResult[i].getValue(columns[4]);
            // 6. Line Unique Key
            arr[6] = objResult[i].getValue(columns[5]);
            // 7. Municipio ID
            arr[7] = objResult[i].getValue(columns[6]);
            // 8. Tipo de Cambio Útil
            if (featureMulti) {
              arr[8] = objResult[i].getValue(columns[10]);
            } else {
              arr[8] = objResult[i].getValue(columns[7]);
            }
            // 9. CST para Ventas (si alicuota es 0)
            arr[9] = objResult[i].getValue(columns[8]);

            ArrReturn.push(arr);
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }

        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    function ObtenerCR20() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = [];

      var savedsearch = search.load({
        id: 'customsearch_lmry_br_gia_cr20'
      });

      if (featuresubs) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var periodFilter = search.createFilter({
        name: 'postingperiod',
        operator: search.Operator.IS,
        values: [paramPeriod]
      });
      savedsearch.filters.push(periodFilter);

      if (featureMulti || featureMulti == 'T') {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [paramMulti]
        });
        savedsearch.filters.push(multibookFilter);
        //7. Monto
        var amountMulti = search.createColumn({
          name: "formulacurrency",
          summary: "SUM",
          formula: "ABS(nvl({accountingtransaction.debitamount},0) - nvl({accountingtransaction.creditamount},0))",
          label: "Monto Multibook"
        });
        savedsearch.columns.push(amountMulti);
      }

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arr = new Array();
            // 0. CR
            arr[0] = '20';
            // 1. Cod. Subitem
            var idOcu = validarNumeros(objResult[i].getValue(columns[3]));
            arr[1] = idOcu;
            // 2. Valor
            if (featureMulti) {
              arr[2] = objResult[i].getValue(columns[7]);
            } else {
              arr[2] = objResult[i].getValue(columns[1]);
            }
            // 3. Propio o ST
            arr[3] = objResult[i].getValue(columns[4]);

            var flegal = '';
            var ocurren = '';
            if (idOcu.substring(3) == '99') {
              flegal = objResult[i].getValue(columns[5]);
              if (flegal == null || flegal == "- None -") {
                flegal = '';
              }
              ocurren = objResult[i].getValue(columns[6]);
              if (ocurren == null || ocurren == "- None -") {
                ocurren = '';
              }
            }
            // 4. F Legal
            arr[4] = flegal;
            // 5. Ocurrencia
            arr[5] = ocurren;

            ArrReturn.push(arr);
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }

        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    function completar_caracter(long, valor, caracter, alineado) {
      if (valor == null) {
        valor = '';
      }

      if (('' + valor).length <= long) {
        if (long != ('' + valor).length) {
          for (var i = ('' + valor).length; i < long; i++) {
            if (alineado) {
              valor = caracter + valor;
            } else {
              valor = valor + caracter;
            }
          }
        } else {
          return valor;
        }
        return valor;
      } else {
        valor = valor.substring(0, long);
        return valor;
      }
    }

    function ObtenerParametrosYFeatures() {
      paramPeriod = objContext.getParameter({
        name: 'custscript_lmry_gia_preformat_period'
      });
      paramSubsi = objContext.getParameter({
        name: 'custscript_lmry_gia_preformat_subsi'
      });
      paramMulti = objContext.getParameter({
        name: 'custscript_lmry_gia_preformat_multi'
      });
      paramFeature = objContext.getParameter({
        name: 'custscript_lmry_gia_preformat_feature'
      });
      paramTipoGIA = objContext.getParameter({
        name: 'custscript_lmry_gia_preformat_gia_type'
      });
      paramTransmitida = objContext.getParameter({
        name: 'custscript_lmry_gia_preformat_trans'
      });
      paramLogID = objContext.getParameter({
        name: 'custscript_lmry_gia_preformat_logid'
      });

      //agregando variables para el special accounting period

      var licenses = libFeature.getLicenses(paramSubsi);
      var feature_pruebita = libFeature.getAuthorization(599, licenses);
      log.error('feature_pruebita',feature_pruebita);

      if(feature_pruebita){
        var SearchPeriodSpecial = search.create({
          type: "customrecord_lmry_special_accountperiod",
          filters:
          [
             ["isinactive","is","F"],
             "AND",
             ["custrecord_lmry_accounting_period","anyof",paramPeriod]
          ],
          columns:
          [
            search.createColumn({name: "name", label: "Name"})
          ]
       });
       var searchResult = SearchPeriodSpecial.run().getRange(0,100);
          if (searchResult.length != 0) {
                var columns = searchResult[0].columns;
                periodname =  searchResult[0].getValue(columns[0]);
          }else{
            var period_temp = search.lookupFields({
                type: search.Type.ACCOUNTING_PERIOD,
                id: paramPeriod,
                columns: ['periodname']
              });
              periodname = period_temp.periodname;
          }
    }else{
      var period_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_PERIOD,
          id: paramPeriod,
          columns: ['periodname']
        });
        periodname = period_temp.periodname;
    }


      if (featureMulti || featureMulti == 'T') {
        var multibookName_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_BOOK,
          id: paramMulti,
          columns: ['name']
        });

        multibookName = multibookName_temp.name;
      }
    }

    function Name_File() {
      var str;
      var refMonth;
      var refYear;
      //agregando variables para el special accounting period

      var licenses = libFeature.getLicenses(paramSubsi);
      var feature_pruebita = libFeature.getAuthorization(599, licenses);
      log.error('feature_pruebita',feature_pruebita);

      if(feature_pruebita){
        var SearchPeriodSpecial = search.create({
          type: "customrecord_lmry_special_accountperiod",
          filters:
          [
             ["isinactive","is","F"],
             "AND",
             ["custrecord_lmry_accounting_period","anyof",paramPeriod]
          ],
          columns:
          [
             search.createColumn({
                name: "formulatext",
                formula: "TO_CHAR({custrecord_lmry_date_ini},'YYYY')",
                label: "Formula (Text)"
             })
          ,
            search.createColumn({
               name: "formulatext",
               formula: "TO_CHAR({custrecord_lmry_date_ini},'MM')",
               label: "Formula (Text)"
            })
         ]
       });

       var searchResult = SearchPeriodSpecial.run().getRange(0,100);
          if (searchResult.length != 0) {
                var columns = searchResult[0].columns;
                refYear =  searchResult[0].getValue(columns[0]);
                refMonth =  searchResult[0].getValue(columns[1]);
          }else{
            var periodLook = search.lookupFields({
            type: search.Type.ACCOUNTING_PERIOD,
            id: paramPeriod,
            columns: ['startdate']
            });
            periodStartDate = periodLook.startdate;
            periodStartDate = format.parse({
              value: periodStartDate,
              type: format.Type.DATE
            });

            if (('' + (periodStartDate.getMonth() + 1)).length == 1) {
              refMonth = '0' + (periodStartDate.getMonth() + 1);
            } else {
              refMonth = periodStartDate.getMonth() + 1;
            }
            refYear = periodStartDate.getFullYear();
          }
    }else{
        var periodLook = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramPeriod,
        columns: ['startdate']
        });
        periodStartDate = periodLook.startdate;
        periodStartDate = format.parse({
          value: periodStartDate,
          type: format.Type.DATE
        });

        if (('' + (periodStartDate.getMonth() + 1)).length == 1) {
          refMonth = '0' + (periodStartDate.getMonth() + 1);
        } else {
          refMonth = periodStartDate.getMonth() + 1;
        }

        refYear = periodStartDate.getFullYear();

      }

      var companyrucTemp = validarNumeros(companyruc);

      if (featuresubs) {
        if (featureMulti) {
          str = 'GIA' + companyrucTemp + '_' + paramSubsi + '_' + refMonth + refYear + '_' + paramMulti + '.prf';
        } else {
          str = 'GIA' + companyrucTemp + '_' + paramSubsi + '_' + refMonth + refYear + '.prf';
        }
      } else {
        if (featureMulti) {
          str = 'GIA' + companyrucTemp + '_' + refMonth + refYear + '_' + paramMulti + '.prf';
        } else {
          str = 'GIA' + companyrucTemp + '_' + refMonth + refYear + '.prf';
        }
      }

      return str;
    }

    function saveFile(str) {
      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_br'
      });

      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        // Extension del archivo
        var NameFile = Name_File();
        //cuando es .DEC  se pone ISO_8859_1 en el encoding
        // Crea el archivo
        var file = fileModulo.create({
          name: NameFile,
          fileType: fileModulo.Type.PLAINTEXT,
          contents: str,
          encoding: fileModulo.Encoding.TXT,
          folder: FolderId
        });

        var idfile = file.save(); // Termina de grabar el archivo
        var idfile2 = fileModulo.load({
          id: idfile
        }); // Trae URL de archivo generado
        // Obtenemos de las preferencias generales el URL de Netsui
        var getURL = objContext.getParameter({
          name: 'custscript_lmry_netsuite_location'
        });
        var urlfile = '';

        if (getURL != '' && getURL != '') {
          urlfile += 'https://' + getURL;
        }

        urlfile += idfile2.url;

        log.error({
          title: 'url',
          details: urlfile
        });

        //Genera registro personalizado como log
        if (idfile) {
          var usuarioTemp = runtime.getCurrentUser();

          var id = usuarioTemp.id;

          var employeename = search.lookupFields({
            type: search.Type.EMPLOYEE,
            id: id,
            columns: ['firstname', 'lastname']
          });

          var usuario = employeename.firstname + ' ' + employeename.lastname;

          if (paramLogID == null || paramLogID == '') {
            var record = recordModulo.create({
              type: 'customrecord_lmry_br_rpt_generator_log',
            });

            //Nombre de Reporte
            record.setValue({
              fieldId: 'custrecord_lmry_ar_rg_transaction',
              value: namereport
            });
            //Nombre de Subsidiaria
            record.setValue({
              fieldId: 'custrecord_lmry_br_rg_subsidiary',
              value: companyname
            });
            //Multibook
            record.setValue({
              fieldId: 'custrecord_lmry_br_rg_multibook',
              value: multibookName
            });

          } else {
            var record = recordModulo.load({
              type: 'customrecord_lmry_br_rpt_generator_log',
              id: paramLogID
            });
          }

          //Nombre de Archivo
          record.setValue({
            fieldId: 'custrecord_lmry_br_rg_name_field',
            value: NameFile
          });
          //Url de Archivo
          record.setValue({
            fieldId: 'custrecord_lmry_br_rg_url_file',
            value: urlfile
          });
          record.setValue({
            fieldId: 'custrecord_lmry_br_rg_period',
            value: periodname
          });
          //Creado Por
          record.setValue({
            fieldId: 'custrecord_lmry_br_rg_employee',
            value: usuario
          });

          var recordId = record.save();
          // Envia mail de conformidad al usuario
          libreria.sendrptuserTranslate(namereport, 3, NameFile, language);

        }
      } else {
        // Debug
        log.error({
          title: 'Creacion de File:',
          details: 'No existe el folder'
        });
      }
    }

    function ObtenerDatosSubsidiaria() {
      var configpage = config.load({
        type: config.Type.COMPANY_INFORMATION
      });

      if (featuresubs) {
        var subsidyName = search.lookupFields({
          type: search.Type.SUBSIDIARY,
          id: paramSubsi,
          columns: ['legalname','taxidnum','custrecord_lmry_br_accounting_icms']
        });

        companyname = subsidyName.legalname;
        companyruc = subsidyName.taxidnum;
        AcredICMS = subsidyName.custrecord_lmry_br_accounting_icms;
        log.debug('AcredICMS',AcredICMS);

      } else {
        companyruc = configpage.getValue('employerid');
        companyname = configpage.getValue('legalname');
      }

      companyruc = companyruc.replace(' ', '');
    }

    return {
      getInputData: getInputData,
      map: map,
      summarize: summarize
    };

  });
