/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_BR_DCTF_MPRD_V2.0.js                        ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Jun 18 2018  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search', 'N/config', 'N/log', 'require', 'N/file', 'N/runtime', 'N/query', "N/format", "N/record", "N/task", "N/url", "./BR_LIBRERIA_MENSUAL/LMRY_BR_Reportes_LBRY_V2.0"],

  function(search, config, log, require, fileModulo, runtime, query, format, recordModulo, task, url, libreria) {
    /**
     * Input Data for processing
     * @return Array,Object,Search,File
     * @since 2019.4
     */
    var objContext = runtime.getCurrentScript();

    var LMRY_script = "LMRY_BR_DCTF_MPRD_V2.0.js";
    //codigos de tipos de notas fiscales electronicas

    //Parametros
    param_RecorID = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_recordid'
    });
    param_Periodo = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_periodo'
    });
    param_Subsi = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_subsidia'
    });
    param_Multi = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_multiboo'
    });
    param_Feature = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_feature'
    });
    param_Num_Recti = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_rectific'
    });
    param_Type_Decla = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_tipo_dec'
    });
    param_Lucro_Conta = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_lucro'
    });
    param_Monto_Adicional = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_monto_ad'
    });
    param_Monto_Excluyente = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_monto_ex'
    });
    param_Excel = objContext.getParameter({
      name: 'custscript_lmry_br_rpt_dctf_mpr_excel'
    });

    //************FEATURES********************
    feature_Subsi = runtime.isFeatureInEffect({
      feature: "SUBSIDIARIES"
    });
    feature_Multi = runtime.isFeatureInEffect({
      feature: "MULTIBOOK"
    });
    feature_Project = runtime.isFeatureInEffect({
      feature: "JOBS"
    });
    feature_magProject = runtime.isFeatureInEffect({
      feature: "ADVANCEDJOBS"
    });

    var language = runtime.getCurrentScript().getParameter({
      name: 'LANGUAGE'
    }).substring(0, 2);

    /* Variables de Tributos para Inventario (Setup) */
    var Receita_pis_inv;
    var Code_receita_pis_inv;
    var Periodicidad_pis_inv;

    var Receita_cofins_inv;
    var Code_receita_cofins_inv;
    var Periodicidad_cofins_inv;

    var Receita_ipi_inv;
    var Periodicidad_ipi_inv;
    var Code_receita_ipi_inv;

    var Filiales;
    var SubsidiariasContempladas;

    var ArrIOF = [];
    var ArrCIDE = [];

    function getInputData() {
      try {
        var intDMinReg = 0;
        var intDMaxReg = 1000;
        var DbolStop = false;
        //para la busqueda de transacciones
        var ArrReturn = new Array();
        var cont = 0;
        var filtro = '';
        var arrAuxiliar = new Array();
        obtenerSetupRptDCTF();

        log.debug('Filiales de la subsidiaria: ' + param_Subsi, Filiales);
        if (Filiales != null && Filiales != '') {
          Filiales = Filiales.split(',');
          SubsidiariasContempladas = Filiales;
          SubsidiariasContempladas.push(param_Subsi); // PARA HACER BUSQUEDA A FILIALES Y MATRIS
          log.debug('Filiales Y subsidiaria', SubsidiariasContempladas);
        } else {
          SubsidiariasContempladas = [param_Subsi];
        }

        obtenerJournalImportacion();

        var savedsearch = search.load({
          /* LatamReady - BR DCTF Taxes MPRD */
          id: 'customsearch_lmry_br_dctf_imp_mprd'
        });
        if (feature_Subsi) {
          var subsidiaryFilter = search.createFilter({
            name: 'subsidiary',
            operator: search.Operator.IS,
            values: SubsidiariasContempladas
          });
          savedsearch.filters.push(subsidiaryFilter);
        }

        var periodFilter = search.createFilter({
          name: 'postingperiod',
          operator: search.Operator.IS,
          values: [param_Periodo]
        });
        savedsearch.filters.push(periodFilter);

        //formula mamadisima
        //(OjO) si aumenta mas tributos solo tendras que modificar aqui
        var formula = "CASE WHEN({custrecord_lmry_br_transaction.custrecord_lmry_br_id_tribute}= '01' OR {custrecord_lmry_br_transaction.custrecord_lmry_br_id_tribute} = '05' OR {custrecord_lmry_br_transaction.custrecord_lmry_br_type}= 'PIS' OR {custrecord_lmry_br_transaction.custrecord_lmry_br_type}='COFINS' OR {custrecord_lmry_br_transaction.custrecord_lmry_br_type}= 'IPI') THEN 1 ELSE 0 END";
        log.debug('formula para impuestos:', formula);
        var featureFilter = search.createFilter({
          name: 'formulatext',
          operator: search.Operator.IS,
          values: 1,
          formula: formula
        });
        savedsearch.filters.push(featureFilter);

        if (feature_Multi) {
          var multibookFilter = search.createFilter({
            name: 'accountingbook',
            join: 'accountingtransaction',
            operator: search.Operator.IS,
            values: [param_Multi]
          });
          savedsearch.filters.push(multibookFilter);
          //columna 15 Exchange MULTIBOOK
          var exchangerate_Column = search.createColumn({
            name: "exchangerate",
            join: "accountingTransaction",
            summary: "GROUP"
          });
          savedsearch.columns.push(exchangerate_Column);
        }

        var searchResult = savedsearch.run();
        while (!DbolStop) {
          var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
          if (objResult != null) {
            if (objResult.length != 1000) {
              DbolStop = true;
            }
            for (var i = 0; i < objResult.length; i++) {
              var columns = objResult[i].columns;
              var arrAuxiliar = new Array();

              // 0. Tipo
              arrAuxiliar[0] = objResult[i].getValue(columns[7]);
              // 1. Codigo Tributo
              if (objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != null) {
                arrAuxiliar[1] = objResult[i].getValue(columns[0]);
              } else {
                arrAuxiliar[1] = '';
              }
              // 2. Tributo
              if (objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != null) {
                arrAuxiliar[2] = objResult[i].getValue(columns[1]);
              } else {
                arrAuxiliar[2] = '';
              }
              // 3. ID Receita
              if (objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != null) {
                arrAuxiliar[3] = objResult[i].getValue(columns[3]);
              } else {
                arrAuxiliar[3] = '';
              }
              // 4.Periodicidad
              if (objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != null) {
                arrAuxiliar[4] = objResult[i].getValue(columns[4]);
              } else {
                arrAuxiliar[4] = '';
              }
              // 5. Monto
              var impLocalCurrency = objResult[i].getValue(columns[14]);
              //log.debug('impLocalCurrency',impLocalCurrency);
              if (impLocalCurrency != null && impLocalCurrency != 0 && impLocalCurrency != '- None -') {
                arrAuxiliar[5] = redondear(impLocalCurrency);
              }else{
                //log.debug('no tiene local currency', arrAuxiliar);
                //para obtener el multibook
                if (feature_Multi) {
                  if (objResult[i].getValue(columns[6]) != '' && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != null) {
                    var exchange_rate_multi = exchange_rate(objResult[i].getValue(columns[6]));
                  } else {
                    var exchange_rate_multi = objResult[i].getValue(columns[15]);
                  }
                } else {
                  var exchange_rate_multi = objResult[i].getValue(columns[10]);
                }

                var impCalculado = redondear(objResult[i].getValue(columns[5])) * exchange_rate_multi;
                arrAuxiliar[5] = redondear(impCalculado);
              }
              //log.debug('monto impuesto',arrAuxiliar[5]);

              // 6. ID del Item
              if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -') {
                arrAuxiliar[6] = objResult[i].getValue(columns[9]);
              } else {
                arrAuxiliar[6] = '';
              }
              //7. Cod. Document Type
              arrAuxiliar[7] = objResult[i].getValue(columns[11]);
              //8. Subisidaria ID
              arrAuxiliar[8] = objResult[i].getValue(columns[8]);

              if (param_Excel == 'T') {
                //9. Nro. Document
                arrAuxiliar[9] = objResult[i].getValue(columns[13]);
              }

              ArrReturn.push(arrAuxiliar);
            }
            if (!DbolStop) {
              intDMinReg = intDMaxReg;
              intDMaxReg += 1000;
            }
          } else {
            DbolStop = true;
          }
        }
        log.debug('Resultado de busqueda', ArrReturn);
        ArrReturn = actualizarTaxResults(ArrReturn);
        /******* IMPUESTOS DE IMPORTACION *******/
        if (ArrIOF.length != 0) {
          ArrReturn = ArrReturn.concat(ArrIOF); //se agregan lineas IOF
        }
        if (ArrCIDE.length != 0) {
          ArrReturn = ArrReturn.concat(ArrCIDE); //se agregan lineas CIDE
        }
        log.debug('Resultado con Taxresult actualizados', ArrReturn);

        return ArrReturn;

      } catch (error) {
        log.error("Error en getInputData", error);
        return [{
          "isError": "T",
          "error": error
        }];
      }
    }

    function map(context) {
      try {
        var arrTransaction = new Array();
        var arrTemp = JSON.parse(context.value);
        var key;
        if (arrTemp["isError"] == "T") {
          context.write({
            key: context.key,
            value: arrTemp
          });
        } else {
          if (param_Excel == 'T') {
            if (arrTemp[5] != 0) { //los que tienen monto 0 no se mandan
              key = context.key;
            }else{
              return false;
            }
          } else {
            if (arrTemp[7] == '99') { //items de servicio
              key = arrTemp[0] + '|' + arrTemp[1] + '|' + arrTemp[3] + '|' + arrTemp[8]; // type|cod. tributo|receita|id subsi
            }
            if (arrTemp[7] == '55') { //items de inventario
              key = arrTemp[0] + '|' + arrTemp[2] + '|' + arrTemp[3] + '|' + arrTemp[8]; // type|tributo|receita|id subsi
            }
            if (arrTemp[0] == 'Journal') { //journals IOF
              key = arrTemp[9] + '|' + arrTemp[2] + '|' + arrTemp[3] + '|' + arrTemp[8]; // idJournal|tributo|receita|id subsi
            }
          }

          if (arrTemp[0] == 'Journal') {
            if (verificarImportacion(Number(arrTemp[6]))) {
              context.write({
                key: key,
                value: {
                  Transaction: arrTemp
                }
              });
            }else{
              log.debug('Alerta en map', 'El vendor del bill de importación, no es del extranjero.');
            }
          } else {
            if (arrTemp[7] == '55' || arrTemp[7] == '99') {
              context.write({
                key: key,
                value: {
                  Transaction: arrTemp
                }
              });
            }
          }

        }
      } catch (e) {
        log.error("Error en map", e);
        context.write({
          key: context.key,
          value: {
            isError: "T",
            error: error
          }
        });
      }
    }

    function reduce(context) {
      try {
        var arrTransaction = new Array();
        var arrAuxiliar = new Array();
        var i_tipo = "";
        var arreglo = context.values;
        var key = context.key;
        var tamano = arreglo.length;
        //log.debug("separacion", '**********************************************');
        //log.debug("key", key);
        for (var i = 0; i < tamano; i++) {
          var obj = JSON.parse(arreglo[i]);
          //log.debug("data reduce", obj);
          arrTransaction.push(obj.Transaction);
        }

        if (param_Excel == 'F') {
          arrAuxiliar[0] = arrTransaction[0][1];
          arrAuxiliar[1] = arrTransaction[0][2];
          arrAuxiliar[2] = arrTransaction[0][3];
          arrAuxiliar[3] = arrTransaction[0][4];
          arrAuxiliar[4] = 0;
          arrAuxiliar[5] = arrTransaction[0][0];
          for (var i = 0; i < arrTransaction.length; i++) {
            //log.debug('montos a sumar:', arrTransaction[i][5]);
            arrAuxiliar[4] += arrTransaction[i][5];
          }
          //log.debug('montos total:', arrAuxiliar[4]);
          arrAuxiliar.push(i_tipo);

          if (arrTransaction[0][0] != 'Journal') {
            arrAuxiliar.push(arrTransaction[0][7]); //se agrega el cod document type
          } else {
            arrAuxiliar.push(arrTransaction[0][9]); //se agrega el id bill importacion
            var arrayLLave = key.split('|');
            key = 'IOF'; //se cambia por el conflico cuando se cambia idioma
            log.debug('llave nueva para IOF transaction', key);
          }

          arrAuxiliar.push(arrTransaction[0][8]); //se agrega id subsidiaria

          if (arrTransaction[0][7] == '99') { //se realiza esto porque hay conflico cuando se cambia de idioma
            var arrayLLave = key.split('|');
            arrayLLave[1] = setearNombreTributo(arrayLLave[1]);
            key = arrayLLave.join('|');
            log.debug('llave nueva para servicio', key);
          }

          log.debug("arreglo en reduce", arrAuxiliar);
          log.debug("llave en reduce", key);

          context.write({
            key: key,
            value: {
              arreglo: arrAuxiliar, //Vector
              Tipo: i_tipo, //tipo de item "Service o Inventory"
              Clase: arrTransaction[0][0] // de Transaccion (Bill o Invoice o Journal)
            }
          });
        } else {
          /********************* EXCEL ********************************/
          if (arrTransaction[0][0] == 'Journal') {
            /* le doy el mismo orden de vector en el archivo temporal*/
            var nroDoc = arrTransaction[0][7];
            var subsidiaria = arrTransaction[0][8];
            arrTransaction[0][7] = arrTransaction[0][9]; //idJournal
            arrTransaction[0][8] = subsidiaria;
            arrTransaction[0][9] = nroDoc;
          }

          //reduce para cada taxresult, por eso solo un elemento
          Auxiliar = arrTransaction[0].join(';') + ';' + i_tipo;
          arrAuxiliar.push(Auxiliar);

          context.write({
            key: context.key,
            value: {
              arreglo: arrAuxiliar,
              Tipo: i_tipo,
              Clase: arrTransaction[0][0]
            }
          });

        }
      } catch (e) {
        log.error("Error en reduce", e);
        context.write({
          key: context.key,
          value: {
            isError: "T",
            error: error
          }
        });
      }
    }

    function summarize(context) {
      try {
        var arrInvoice = new Array();
        var arrBill = new Array();
        var arrJournal = new Array();
        var arrArchivo = new Array();

        context.output.iterator().each(function(key, value) {
          var obj = JSON.parse(value);
          var clase = obj.Clase;
          if (clase == 'VendBill') {
            arrBill.push(obj.arreglo);
          } else if (clase == 'CustInvc') {
            arrInvoice.push(obj.arreglo);
          } else if (clase == 'Journal') {
            arrJournal.push(obj.arreglo);
          }
          return true;
        });
        log.debug('Array Bills Summarize', arrBill);
        log.debug('Array Invoices Summarize', arrInvoice);
        log.debug('Array Journal Summarize', arrJournal);

        //Generamos el archivirigillo
        if (param_Excel == 'F') {
          var StrArchivo = '';
          for (var i = 0; i < arrInvoice.length; i++) {
            if (i == arrInvoice.length - 1) {
              StrArchivo += arrInvoice[i].join(';');
            } else {
              StrArchivo += arrInvoice[i].join(';') + '|';
            }
          }
          StrArchivo += '@';
          for (var j = 0; j < arrBill.length; j++) {
            if (j == arrBill.length - 1) {
              StrArchivo += arrBill[j].join(';');
            } else {
              StrArchivo += arrBill[j].join(';') + '|';
            }
          }
          StrArchivo += '@';
          for (var j = 0; j < arrJournal.length; j++) {
            if (j == arrJournal.length - 1) {
              StrArchivo += arrJournal[j].join(';');
            } else {
              StrArchivo += arrJournal[j].join(';') + '|';
            }
          }
        } else {

          var StrArchivo = '';
          for (var i = 0; i < arrInvoice.length; i++) {
            if (i == arrInvoice.length - 1) {
              StrArchivo += arrInvoice[i].toString();
            } else {
              StrArchivo += arrInvoice[i].toString() + '|';
            }
          }
          StrArchivo += '@';
          for (var j = 0; j < arrBill.length; j++) {
            if (j == arrBill.length - 1) {
              StrArchivo += arrBill[j].toString();
            } else {
              StrArchivo += arrBill[j].toString() + '|';
            }
          }
          StrArchivo += '@';
          for (var j = 0; j < arrJournal.length; j++) {
            if (j == arrJournal.length - 1) {
              StrArchivo += arrJournal[j].toString();
            } else {
              StrArchivo += arrJournal[j].toString() + '|';
            }
          }
        }

        var idFile = SaveFileAux(StrArchivo);
        log.debug('llamando schedule');
        LlamarSchedule(idFile);
      } catch (error) {
        log.error("Error en summarize", error);
        libreria.sendemailTranslate(error, LMRY_script, language);
        NoData(true);
      }
    }

    function verificarImportacion(idBill) {
      var esImportacion = false;

      var bill = search.lookupFields({
        type: search.Type.VENDOR_BILL,
        id: idBill,
        columns: ['entity']
      });

      var vendor = search.lookupFields({
        type: search.Type.VENDOR,
        id: bill.entity[0].value,
        columns: ['custentity_lmry_countrycode']
      });

      if (vendor.custentity_lmry_countrycode != '1058') {//CODE BRAZIL
        esImportacion = true;
      }

      return esImportacion;
    }

    function obtenerJournalImportacion() {
      var arrayIOF = [];
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;

      var savedsearch = search.load({
        /* LatamReady BR - Import Taxes DCTF */
        id: 'customsearch_lmry_br_dctf_import_taxes'
      });

      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: SubsidiariasContempladas
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var formulaPeriod = "CASE WHEN {custbody_lmry_cl_period.custrecord_lmry_cl_period_fact_actual.id}='"+param_Periodo+"' THEN 1 ELSE 0 END";
      log.debug('formulaPeriod',formulaPeriod);
      var periodFilter = search.createFilter({
        name: 'formulanumeric',
        formula: formulaPeriod,
        operator: search.Operator.EQUALTO,
        values: 1
      });
      savedsearch.filters.push(periodFilter);

      if (feature_Multi) {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch.filters.push(multibookFilter);
        //10
        var montoMultibook = search.createColumn({
          name: "formulacurrency",
          formula: "nvl({accountingtransaction.debitamount},0)",
          label: "Monto con Multibook"
        });
        savedsearch.columns.push(montoMultibook);
      }

      var searchresult = savedsearch.run();

      while (!DbolStop) {

        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
        if (objResult != null && objResult != 0) {
          var intLength = objResult.length;

          if (intLength != 1000) {
            DbolStop = true;
          }
          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            arrAuxiliar = new Array();

            for (var j = 0; j < 10; j++) { //no queremos que se guarden los tipos de cambio
              if (j == 5) { //monto
                if (feature_Multi) {
                  arrAuxiliar[j] = Number(objResult[i].getValue(columns[10]));
                  arrAuxiliar[j] = redondear(arrAuxiliar[j]);
                } else {
                  arrAuxiliar[j] = Number(objResult[i].getValue(columns[j]));
                  arrAuxiliar[j] = redondear(arrAuxiliar[j]);
                }
              } else {
                arrAuxiliar[j] = objResult[i].getValue(columns[j]);
              }
            }

            if (arrAuxiliar[2] == 'CIDE') {
              ArrCIDE.push(arrAuxiliar);
            }else{
              ArrIOF.push(arrAuxiliar);
            }
          }

          log.debug('arreglos de iof', arrayIOF);
          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
        } else {
          DbolStop = true;
        }
      }

      return arrayIOF;
    }

    function redondear(number) {
      return Math.round(Number(number) * 100) / 100;
    }

    /** Seteara lo siguiente:
     * Codigo de Tributo en caso no tenga
     * Receitas y Periodicidad solo para Tributos Inventario
     * retorna arreglo actualizado
     */
    function actualizarTaxResults(arreglo) {
      for (var i = 0; i < arreglo.length; i++) {
        if (arreglo[i][2] == 'IPI') {
          arreglo[i][1] = '03';
          if (arreglo[i][7] == '55') {
            arreglo[i][3] = Code_receita_ipi_inv;
            //arreglo[i][] = Code_receita_ipi_inv;
            arreglo[i][4] = Periodicidad_ipi_inv;
          }
        }
        if (arreglo[i][2] == 'PIS') {
          arreglo[i][1] = '06';
          if (arreglo[i][7] == '55') {
            arreglo[i][3] = Code_receita_pis_inv;
            //arreglo[i][] = Code_receita_pis_inv;
            arreglo[i][4] = Periodicidad_pis_inv;
          }
        }
        if (arreglo[i][2] == 'COFINS') {
          arreglo[i][1] = '07';
          if (arreglo[i][7] == '55') {
            arreglo[i][3] = Code_receita_cofins_inv;
            //arreglo[i][] = Code_receita_cofins_inv;
            arreglo[i][4] = Periodicidad_cofins_inv;
          }
        }
      }
      return arreglo;
    }

    function setearNombreTributo(codigoTributo) {
      var tributoNombre = '';
      if (codigoTributo == '03') {
        tributoNombre = 'IPI'
      }
      if (codigoTributo == '06') {
        tributoNombre = 'PIS'
      }
      if (codigoTributo == '07') {
        tributoNombre = 'COFINS'
      }
      if (codigoTributo == '01') {
        tributoNombre = 'IRPJ'
      }
      if (codigoTributo == '05') {
        tributoNombre = 'CSLL'
      }

      return tributoNombre;
    }

    function obtenerSetupRptDCTF() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;

      var DbolStop = false;
      var arrAuxiliar = new Array();

      var _cont = 0;

      var savedsearch = search.create({
        type: 'customrecord_lmry_br_setup_rpt_dctf',
        columns: [
          //0 Latam - BR Receita - PIS
          search.createColumn({
            name: "custrecord_lmry_br_receita_pis",
            label: "Latam - BR Receita - PIS"
          }),
          //1 Latam - BR Code Receita - PIS
          search.createColumn({
            name: "custrecord_lmry_br_code_receita_pis",
            label: "Latam - BR Code Receita - PIS"
          }),
          //2 Latam - BR Id Periodicity - PIS
          search.createColumn({
            name: "custrecord_lmry_br_periodicity_pis",
            label: "Latam - BR Id Periodicity - PIS"
          }),
          //3  Latam - BR Receita - COFINS
          search.createColumn({
            name: "custrecord_lmry_br_receita_cofins",
            label: "  Latam - BR Receita - COFINS"
          }),
          //4 Latam - BR Code Receita - COFINS
          search.createColumn({
            name: "custrecord_lmry_br_code_receita_cofins",
            label: "Latam - BR Code Receita - COFINS"
          }),
          //5 Latam - BR Id Periodicity - COFINS
          search.createColumn({
            name: "custrecord_lmry_br_periodicity_cofins",
            label: "Latam - BR Id Periodicity - COFINS"
          }),
          //6
          search.createColumn({
            name: "custrecord_lmry_br_receita_ipi",
            label: "Latam - BR Receita - IPI"
          }),
          //7
          search.createColumn({
            name: "custrecord_lmry_br_code_receita_ipi",
            label: "Latam - BR Code Receita - IPI"
          }),
          //8
          search.createColumn({
            name: "custrecord_lmry_br_periodicity_ipi",
            label: "Latam - BR Periodicity - IPI"
          }),
          //9
          search.createColumn({
            name: "custrecord_lmry_br_filiales",
            label: "Filiales"
          })
        ]
      });

      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'custrecord_lmry_br_rpt_subsidiary',
          operator: search.Operator.ANYOF,
          values: [param_Subsi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var searchresult = savedsearch.run();
      var objResult = searchresult.getRange(0, 1);
      //log.debug('Setup Receitas Inv', objResult);

      if (objResult != null && objResult.length != 0) {
        var columns = objResult[0].columns;

        ReceitaId_pis_inv = objResult[0].getValue(columns[0]);
        Code_receita_pis_inv = objResult[0].getValue(columns[1]);
        Periodicidad_pis_inv = objResult[0].getValue(columns[2]);

        ReceitaId_cofins_inv = objResult[0].getValue(columns[3]);
        Code_receita_cofins_inv = objResult[0].getValue(columns[4]);
        Periodicidad_cofins_inv = objResult[0].getValue(columns[5]);

        ReceitaId_ipi_inv = objResult[0].getValue(columns[6]);
        Code_receita_ipi_inv = objResult[0].getValue(columns[7]);
        Periodicidad_ipi_inv = objResult[0].getValue(columns[8]);

        Filiales = objResult[0].getValue(columns[9]);

      } else {
        log.debug("Alerta de Setup BR", 'No se tiene configuracion del Setup para la subsidiaria seleccionada.');
      }
    }

    function LlamarSchedule(id_archivo) {
      var params = {};
      if (param_Excel == 'T') {
        params['custscript_lmry_br_dctf_xls_periodo'] = param_Periodo;
        params['custscript_lmry_br_dctf_xls_featureid'] = param_Feature;
        if (feature_Subsi) {
          params['custscript_lmry_br_dctf_xls_subsid'] = param_Subsi;
        }
        if (feature_Multi) {
          params['custscript_lmry_br_dctf_xls_multibook'] = param_Multi;
        }
        //parametro de archivo temporal
        params['custscript_lmry_br_dctf_xls_fileid'] = id_archivo;
        params['custscript_lmry_br_dctf_xls_tipo_decla'] = param_Type_Decla;
        params['custscript_lmry_br_dctf_xls_num_rec'] = param_Num_Recti;
        params['custscript_lmry_br_dctf_xls_logid'] = param_RecorID;
        params['custscript_lmry_br_dctf_xls_lucro'] = param_Lucro_Conta;
        params['custscript_lmry_br_dctf_xls_monto_adic'] = param_Monto_Adicional;
        params['custscript_lmry_br_dctf_xls_monto_excl'] = param_Monto_Excluyente;

        id = 'customscript_lmry_br_dctf_xls_schdl';
        deploy = 'customdeploy_lmry_br_dctf_xls_schdl';
      } else {
        params['custscript_lmry_br_dctf_dec_periodo'] = param_Periodo;
        params['custscript_lmry_br_dctf_dec_featureid'] = param_Feature;
        if (feature_Subsi) {
          params['custscript_lmry_br_dctf_dec_subsi'] = param_Subsi;
        }
        if (feature_Multi) {
          params['custscript_lmry_br_dctf_dec_multi'] = param_Multi;
        }
        //parametro de archivo temporal
        params['custscript_lmry_br_dctf_dec_archivo'] = id_archivo;
        params['custscript_lmry_br_dctf_dec_decla'] = param_Type_Decla;
        params['custscript_lmry_br_dctf_dec_recti'] = param_Num_Recti;
        params['custscript_lmry_br_dctf_dec_recorid'] = param_RecorID;
        params['custscript_lmry_br_dctf_dec_lucro_conta'] = param_Lucro_Conta;
        params['custscript_lmry_br_dctf_dec_monto_adic'] = param_Monto_Adicional;
        params['custscript_lmry_br_dctf_dec_monto_excluy'] = param_Monto_Excluyente;

        id = 'customscript_lmry_br_dctf_dec_schdl';
        deploy = 'customdeploy_lmry_br_dctf_dec_schdl';
      }

      var taskScript = task.create({
        taskType: task.TaskType.SCHEDULED_SCRIPT,
        scriptId: id,
        deploymentId: deploy,
        params: params
      });
      taskScript.submit();
    }

    function NoData(hayError) {
      var usuarioTemp = runtime.getCurrentUser();
      var id = usuarioTemp.id;
      var employeename = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: id,
        columns: ['firstname', 'lastname']
      });
      var usuario = employeename.firstname + ' ' + employeename.lastname;

      if (hayError) {
        var message = "Ocurrio un error inesperado en la ejecucion del reporte.";
      } else {
        var message = "No existe informacion para los criterios seleccionados.";
      }

      var record = recordModulo.load({
        type: 'customrecord_lmry_br_rpt_generator_log',
        id: param_RecorID
      });

      //Nombre de Archivo
      record.setValue({
        fieldId: 'custrecord_lmry_br_rg_name_field',
        value: message
      });
      //Creado Por
      record.setValue({
        fieldId: 'custrecord_lmry_br_rg_employee',
        value: usuario
      });

      var recordId = record.save();
    }

    function QuitarVacios(arreglo) {
      var arrAuxiliar = new Array();
      for (var i = 0; i < arreglo.length; i++) {
        if (arreglo[i][0] != '') {
          arrAuxiliar.push(arreglo[i]);
        }
      }
      return arrAuxiliar;
    }

    function SaveFileAux(contenido) {
      var folderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_br'
      });
      // Almacena en la carpeta de Archivos Generados
      if (folderId != '' && folderId != null) {
        // Extension del archivo
        var fileName = 'ArchivoTemporalDCTF.txt';
        // Crea el archivo
        var file_aux = fileModulo.create({
          name: fileName,
          fileType: fileModulo.Type.PLAINTEXT,
          contents: contenido,
          encoding: fileModulo.Encoding.MAC_ROMAN,
          folder: folderId
        });

        var idFile = file_aux.save(); // Termina de grabar el archivo
      } else {

      }
      return idFile;
    }

    function exchange_rate(exchangerate) {
      var auxiliar = ('' + exchangerate).split('&');
      var final = '';

      if (feature_Multi) {
        var id_libro = auxiliar[0].split('|');
        var exchange_rate = auxiliar[1].split('|');

        for (var i = 0; i < id_libro.length; i++) {
          if (Number(id_libro[i]) == Number(param_Multi)) {
            final = exchange_rate[i];
            break;
          } else {
            final = exchange_rate[0];
          }
        }
      } else {
        final = auxiliar[1];
      }
      return final;
    }

    return {
      getInputData: getInputData,
      map: map,
      reduce: reduce,
      summarize: summarize
    }

  });
