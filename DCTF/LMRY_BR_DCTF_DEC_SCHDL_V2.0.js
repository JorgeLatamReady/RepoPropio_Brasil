/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for Inventory Balance Library                  ||
||                                                              ||
||  File Name: LMRY_BR_DCTF_DEC_SCHDL_V2.0.js                   ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Apr 27 2020  LatamReady    Use Script 2.0           ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.x
 * @NScriptType ScheduledScript
 * @NModuleScope Public
 */
define(["N/record", "N/runtime", "N/file", "N/email", "N/search", "N/format",
    "N/log", "N/config", "N/task", "./BR_LIBRERIA_MENSUAL/LMRY_BR_Reportes_LBRY_V2.0.js",
    "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_libSendingEmailsLBRY_V2.0.js"
  ],

  function(recordModulo, runtime, fileModulo, email, search, format, log,
    config, task, libreria, libFeature) {

    var objContext = runtime.getCurrentScript();
    // Nombre del Reporte
    var namereport = "Declaración de Débitos y Créditos Tributarios Federales SCHDL";
    var LMRY_script = 'LMRY_BR_DCTF_DEC_SCHDL_V2.0.js';

    //PARAMETROS
    var param_RecorID = null;
    var param_Periodo = null;
    var param_Subsi = null;
    var param_Type_Decla = null;
    var param_Num_Recti = null;
    var param_Multi = null;
    var param_Feature = null
    var param_Lucro_Conta = null;
    var param_Monto_Adicional = null;
    var param_Monto_Excluyente = null;
    //FEATURES
    var feature_Subsi = null;
    var feature_Multi = null;
    var featAccountingSpecial = null;
    //DATOS SUBSIDIARIA
    var companyname = null;
    var companyruc = null;
    var CNPJ = null;
    var uf_domicilio = null;
    var address = null;
    var barrio = '';
    var num_dir = '';
    var complemento = '';
    var city = '';
    var correo = '';
    var uf_subsi = null;
    var zip = null;
    var cef = null;
    var phone = null;
    var percentage_receta = null;
    var ddd_subsi = null;
    //Period enddate
    var periodenddate = null;
    var mes_date = null;
    var dia_date = null;
    var anio_date = null;
    var mes_ini = null;
    var dia_ini = null;
    var anio_ini = null;
    var periodname = null;
    var dia_actual = null;
    //Valores del SetupDCTF
    var cod_decla_Situacion = null;
    var cod_cadastro_Situacion = null;
    var cod_form_tribut = null;
    var cod_califica = null;
    var balance_suspensao = null;
    var pj_debitos_scp = null;
    var pj_simple_nacional = null;
    var pj_cprb = null;
    var pj_inactivo_mes = null;
    var variable_monetaria = null;
    var pj_mes_declara = null;
    var opcion_ref = null;
    var reg_pis_confis = null;
    var legal_representante = null;
    var ddd_tel_representante = null;
    var name_responsable = null;
    var ddd_tel_responsable = null;
    var id_account_payroll = null;
    var id_tributo_payroll = null;
    var id_receta_payroll = null;
    var id_periodicidad_payroll = null;
    var monto_limite = null;
    var porc_alicuota_irpj = null;
    var porc_adicional_irpj = null;
    var porc_alicuota_csll = null;
    var id_receta_irpj = null;
    var id_periodicidad_irpj = null;
    var id_receta_csll = null;
    var id_periodicidad_csll = null;
    //cambios de  27/03/2019
    var id_tribute_wht_irrf_repres = null;
    var id_receita_wht_irrf_repres = null;
    var periodicidad_irrf_repres = null;
    var id_item_repres_legal = null;
    var Acum_wht_min = null;
    var Acum_imp_min = null;
    var Monto_min_imp = 0;

    var type_concept_journal = null;
    var name_type_concepto = null;
    var monto_payroll = null;
    var monto_payroll_CPRB = null;
    var IdArchivoAnt = '';
    var InternalidRecordActual = '';
    var strArchivo = '';
    //Nombre de libro contable
    var multibookName = '';
    var version = '';

    var language = runtime.getCurrentScript().getParameter({
      name: 'LANGUAGE'
    }).substring(0, 2);

    var ArrSetupDCTF_Sales = new Array();
    var ArrSetupDCTF_Purchases = new Array();
    var ArrSetupDCTF_Sales_Inv = new Array();
    var ArrSetupDCTF_Purchases_Inv = new Array();
    var Arr_WHT_Item = new Array();
    var Arr_WHT_No_Item = new Array();
    var ArrIOFJournal = new Array();
    var ArrCIDEJournal = new Array();
    var ArrIOFBillPay = new Array();
    var ArrCIDEBillPay = new Array();
    var ArrTempJournalImportacion = new Array();
    var ArrJournalR11 = new Array();
    var LineasR12 = [];
    var LineasR14 = [];
    var ArrImpMinimos = new Array();
    var PeriodAnt = '';
    var ArrR11Nomina = new Array();
    var EPayLines = new Array();
    var EPayLinesNomina = new Array();
    var EPayLinesPago = new Array();
    var Filiales;
    var SubsidiariasContempladas;

    var Var_Acount_Payroll = null;
    var cont_reg = 0; //PARA T9

    function execute(context) {
      try {
        log.debug('Inicio', 'Lipa estuvo aqui');
        ObtenerParametrosYFeatures();
        ObtnerSetupRptDCTF();
        ObtenerDatosSubsidiaria();

        if (Filiales != '' && Filiales != null) {
          Filiales = Filiales.split(',');
          log.debug('Filiales', Filiales);
          SubsidiariasContempladas = Filiales;
          SubsidiariasContempladas.push(param_Subsi); //se pushea en Filiales tmb, raro
        } else {
          SubsidiariasContempladas = [param_Subsi];
        }

        GenerarArreglos(); //obtiene data del archivo temporal
        obtenerDatosCreditFile(); //obtiene creditos anteriores
        /********************* CARGADO DE JOURNALS R11 ****************************/
        var lineasPagoImpuestos = CargarJournalR11(5); //ID Concepto: Pagos de Impuesto
        var lineasNomina = CargarJournalR11(25); //ID Concepto: Impostos de Nomina
        var pagosTotales = lineasPagoImpuestos.concat(lineasNomina);
        CargarR11(pagosTotales); //formatea la data de pagos a la estructura R11
        ArrJournalR11 = agruparPagos(ArrJournalR11);
        ArrR11Nomina = agruparPagos(ArrR11Nomina);
        formatLinesR11(EPayLines);//formate la data de pagos de e payment para R11
        ArrJournalR11 = ArrJournalR11.concat(EPayLinesPago);
        var strNomina = '';
        strNomina += armarLineasNomina(ArrR11Nomina); //arma lineas r11 y r10 para el reporte
        strNomina += armarLineasNomina(EPayLinesNomina); //arma lineas r11 y r10 para el reporte
        /********************* CARGADO DE COMPENSACIONES Y SUSPENSIONES (R12/R14) *******************/
        var ArrJournalR12 = CargarJournals('R12');
        LineasR12 = CargarLineasR12_R14(ArrJournalR12, 'R12');
        var ArrJournalR14 = CargarJournals('R14');
        LineasR14 = CargarLineasR12_R14(ArrJournalR14, 'R14');
        LineasR14 = agruparR14(LineasR14);
        /*******************************  IMPUESTOS ************************************************/
        var strR10IPIFiliales = separarIPI_filiales(); //arma r10 IPI(filiales y matriz) y retira dichas lineas de los arreglos
        //Se agrupan los arreglos y acumulan montos
        ArrSetupDCTF_Sales_Inv = acumularMontosMatriz(ArrSetupDCTF_Sales_Inv);
        ArrSetupDCTF_Purchases_Inv = acumularMontosMatriz(ArrSetupDCTF_Purchases_Inv);
        ArrSetupDCTF_Sales = acumularMontosMatriz(ArrSetupDCTF_Sales);
        ArrSetupDCTF_Purchases = acumularMontosMatriz(ArrSetupDCTF_Purchases);

        var strR10Impuestos = '';
        var impuestos;
        if (cod_form_tribut == '1') {
          impuestos = calcularRealEstimativo(ArrSetupDCTF_Sales_Inv, ArrSetupDCTF_Purchases_Inv, ArrSetupDCTF_Sales, ArrSetupDCTF_Purchases);
          impuestos = eliminarMontosCeros(impuestos);
        } else {
          impuestos = calcularPresumido(ArrSetupDCTF_Sales_Inv, ArrSetupDCTF_Purchases_Inv, ArrSetupDCTF_Sales, ArrSetupDCTF_Purchases);
          impuestos = eliminarMontosCeros(impuestos);
        }

        if (Acum_imp_min) {
          impuestos = actualizarMontosMinimos(impuestos);
        } else {
          impuestos = eliminarMinimos(impuestos);
        }
        strR10Impuestos = armarR10(impuestos, CNPJ);

        /********* IMPUESTOS DE IMPORTACION **************/
        var strR10Importacion = ''; //se fusionan lineas de proceso automatico y manual para agrupar
        var iofTotal = ArrIOFBillPay.concat(ArrIOFJournal);
        var cideTotal = ArrCIDEBillPay.concat(ArrCIDEJournal);
        if (cideTotal.length != 0 || iofTotal.length != 0) {
          iofTotal = acumularMontosMatriz(iofTotal);
          strR10Importacion += armarLineasPorFilial(cideTotal);
          strR10Importacion += armarR10(prepararMatriz(iofTotal), '');
        }
        log.debug('cadenas r10 importacion', strR10Importacion);
        /****************************** RETENCIONES (de BIlls) ************************************/
        ObtenerWHT();
        Arr_WHT_No_Item = acumularRetencionMatriz(Arr_WHT_No_Item);
        Arr_WHT_Item = acumularIRRF(Arr_WHT_Item);
        log.debug('despues de funcion - item', Arr_WHT_Item);

        var retencionesTotal = prepararRetenciones();
        if (Acum_wht_min) {
          retencionesTotal = actualizarMontosMinimos(retencionesTotal);
        } else {
          retencionesTotal = eliminarMinimos(retencionesTotal);
        }
        var strRetenciones = armarR10(retencionesTotal, '');
        /*******************************      PAYROLL           ************************************/
        CargarPayRoll();
        CargarPayRollCPRB();
        var strPayroll = armarR10(prepararPayroll(), '');
        /******************************  ARMADO DE ARCHIVO   ****************************************/
        CargarHeader();
        CargarDatosIniciales();
        CargaDatosRegistrales();
        CargaDatosResponsable();
        strArchivo += strR10Impuestos + strR10IPIFiliales + strR10Importacion + strPayroll + strNomina + strRetenciones;
        CargarFinal();

        /** ACTUALIZACION BR CREDIT FILE MONTOS MINIMOS **/
        if (Acum_imp_min || Acum_wht_min) {
          actualizarCreditFileRecord();
        }

        if (cont_reg > 4) {
          SaveFile();
        } else {
          NoData(false);
        }
      } catch (error) {
        log.error("Error en schedule dec", error);
        libreria.sendemailTranslate(error, LMRY_script, language);
        NoData(true);
      }
    }

    function definirPeriodoApuracao(periodicidad, campo_11) {
      var json = {
        campo9: '00',
        campo10: '00',
        campo11: '00'
      };

      if (periodicidad == 'A') {
        var anioApuracao = anio_date;
        if (cod_cadastro_Situacion == '0') {
          anioApuracao = Number(anioApuracao) - 1;
          anioApuracao += '';
        }
        json.campo9 = anioApuracao;
      } else if (periodicidad == 'M') {
        json.campo9 = anio_date;
        json.campo10 = mes_date;
      } else {
        json.campo9 = anio_date;
        json.campo10 = mes_date;
        json.campo11 = campo_11;
      }

      return json;
    }

    function armarLineasNomina(arrayData) {
      var strResult = '';
      var long = arrayData.length;
      var i = 0;
      log.debug('lineas de nomina', arrayData);
      while (i < long) {
        var matrizTemporal = [];
        var j = i + 1;
        var montoDebito = Number(arrayData[i][22]);
        matrizTemporal.push(arrayData[i]);

        var esPorFilial = false;
        if (arrayData[i][5] == '03' || arrayData[i][5] == '09') { //IPI y CIDE
          esPorFilial = true;
        }
        /******** SE AGRUPAN LOS ELEMENTOS EN UNA MATRIZ TEMPORAL (PARA SACAR EL R10) ****/
        while (j < long) {
          if (esPorFilial) { //tributo - receita - periodic_campo11 - subsi cnpj
            if (arrayData[i][5] == arrayData[j][5] && arrayData[i][6] == arrayData[j][6] && arrayData[i][10] == arrayData[j][10] && arrayData[i][15] == arrayData[j][15]) {
              montoDebito += Number(arrayData[j][22]);
              matrizTemporal.push(arrayData[j]);
              arrayData.splice(j, 1);
              long--;
            } else {
              j++;
            }
          } else { //tributo - receita - periodic_campo11
            if (arrayData[i][5] == arrayData[j][5] && arrayData[i][6] == arrayData[j][6] && arrayData[i][10] == arrayData[j][10]) {
              montoDebito += Number(arrayData[j][22]);
              matrizTemporal.push(arrayData[j]);
              arrayData.splice(j, 1);
              long--;
            } else {
              j++;
            }
          }
        }
        log.debug('matriz agrupada nomina', matrizTemporal);
        /******************* ARMAR LINEAS R10 Y R11 ******************/
        /******* R10 *******/
        var columna1 = 'R10';
        var key = arrayData[i].slice(1, 14);
        key = key.join('');

        var columna15 = completar(14, ValidaGuion(montoDebito), '0', true);
        var columna16 = '0';
        if (arrayData[i][5] == '01' || arrayData[i][5] == '05') { //IRPJ O CSLL
          if (balance_suspensao == 'F' || balance_suspensao == false) {
            columna16 = '1'; ////*****Setup
          }
        }
        var columna17 = '0';
        var columna18 = '0';
        var columna19 = ''; /** setup **/
        pj_debitos_scp ? columna19 = '1' : columna19 = '0';
        //Reservdo
        var columna20 = completar(10, '', ' ', false);
        //Delimitador
        var columna21 = '\r\n';

        strResult += columna1 + key + columna15 + columna16 + columna17 + columna18 + columna19 + columna20 + columna21;
        cont_reg++;
        /****** R11 *********/
        for (var k = 0; k < matrizTemporal.length; k++) {
          strResult += matrizTemporal[k].join('');
          cont_reg++;
        }

        i++;
      }

      return strResult;
    }

    function armarLineasPorFilial(dataJournal) {
      var matrizActualizada = [];
      var long = dataJournal.length;
      var i = 0;

      while (i < long) {
        var montoAcumulado = Number(dataJournal[i][4]);
        var j = i + 1;
        while (j < long) { //acumulando por cod.tributo - receita - subsidiaria
          if (dataJournal[i][0] == dataJournal[j][0] && dataJournal[i][2] == dataJournal[j][2] && dataJournal[i][8] == dataJournal[j][8]) {
            montoAcumulado += Number(dataJournal[j][4]);
            dataJournal.splice(j, 1);
            long--;
          } else {
            j++;
          }
        }
        dataJournal[i][4] = montoAcumulado;
        matrizActualizada.push(dataJournal[i]);
        i++;
      }

      log.debug('matriz acumulada CIDE', matrizActualizada);
      /* Se ordenara la matriz por filial para armar r10 */
      var strR10 = '';
      var long = matrizActualizada.length;
      var i = 0;

      while (i < long) {
        var matrizOrdenada = [];
        matrizOrdenada.push(matrizActualizada[i]);
        var j = i + 1;
        while (j < long) {
          if (matrizActualizada[i][8] == matrizActualizada[j][8]) {
            matrizOrdenada.push(matrizActualizada[j]);
            matrizActualizada.splice(j, 1);
            long--;
          } else {
            j++;
          }
        }

        var subsi_temp = search.lookupFields({
          type: search.Type.SUBSIDIARY,
          id: Number(matrizOrdenada[0][8]),
          columns: ['taxidnum']
        });
        var cnpjFilial = subsi_temp.taxidnum;
        log.debug('MATRIZ importacion por filial', matrizOrdenada);
        strR10 += armarR10(prepararMatriz(matrizOrdenada), cnpjFilial);

        i++;
      }

      log.debug('cadena CIDE', strR10);
      return strR10;
    }

    function acumularRetencionMatriz(matrizGeneral) {
      var matrizActualizada = [];
      var long = matrizGeneral.length;
      var i = 0;

      while (i < long) {
        var montoAcumulado = Number(matrizGeneral[i][4]);
        var j = i + 1;
        while (j < long) { //acumulando por cod. Tributo - periodicidad - receita
          if (matrizGeneral[i][0] == matrizGeneral[j][0] && matrizGeneral[i][5] == matrizGeneral[j][5] && matrizGeneral[i][3] == matrizGeneral[j][3]) {
            montoAcumulado += Number(matrizGeneral[j][4]);
            matrizGeneral.splice(j, 1);
            long--;
          } else {
            j++;
          }
        }
        matrizGeneral[i][4] = montoAcumulado;
        matrizActualizada.push(matrizGeneral[i]);
        i++;
      }

      log.debug('matriz actualizada wht', matrizActualizada);
      return matrizActualizada;
    }

    function obtenerPeriodoAnt(anio, mes) {
      var mes_ant;
      var anio_ant;

      if (mes == 1) {
        mes_ant = '12';
        anio_ant = anio - 1;
      } else {
        mes_ant = mes - 1;
        if ((mes_ant + '').length == 1) {
          mes_ant = '0' + mes_ant
        }
        anio_ant = anio
      }
      mes_ant = mes_ant + '';
      anio_ant = anio_ant + '';

      return mes_ant + anio_ant
    }

    function obtenerDatosCreditFile() {
      var strPeriodActual = mes_date + '' + anio_date;

      var busq_creditFile = search.create({
        type: 'customrecord_lmry_br_credit_file',
        filters: [
          ["custrecord_lmry_br_credit_subsi", "anyof", param_Subsi],
          "AND",
          ["custrecord_lmry_br_credit_feature", "anyof", param_Feature]
        ],
        columns: ['custrecord_lmry_br_credit_idfile', 'internalid', 'custrecord_lmry_br_credit_periodo']
      });

      var formula = "CASE WHEN {custrecord_lmry_br_credit_periodo}='" + PeriodAnt + "' OR {custrecord_lmry_br_credit_periodo}='" + strPeriodActual + "' THEN 1 ELSE 0 END";
      var periodoFilter = search.createFilter({
        name: 'formulatext',
        formula: formula,
        operator: search.Operator.IS,
        values: 1
      });
      busq_creditFile.filters.push(periodoFilter);

      var result = busq_creditFile.run().getRange(0, 1000);

      if (result.length != 0 && result != null) {
        for (var i = 0; i < result.length; i++) {
          var columns = result[i].columns;
          var periodo = result[i].getValue(columns[2]);

          if (periodo == PeriodAnt) {
            IdArchivoAnt = result[i].getValue(columns[0]);
          }
          if (periodo == (mes_date + anio_date)) {
            InternalidRecordActual = result[i].getValue(columns[1]);
          }
        }
      }
      log.debug('id arch anterior', IdArchivoAnt);
      log.debug('id record credit actual', InternalidRecordActual);
      if (IdArchivoAnt != '') {
        var string = fileModulo.load({
          id: IdArchivoAnt
        });
        var contenido = string.getContents()
        contenido = contenido.split('\r\n');

        for (var i = 0; i < contenido.length; i++) {
          ArrImpMinimos.push(contenido[i].split('|'));
        }
      }

      log.debug('impuestos minimos anteriores', ArrImpMinimos);
    }

    function actualizarMontosMinimos(arrayImpuestos) {
      if (IdArchivoAnt == '') { // SI NO HAY DATA ANTERIOR
        var long = arrayImpuestos.length;
        var i = 0;
        while (i < long) {
          if (arrayImpuestos[i][3] < Monto_min_imp) {
            ArrImpMinimos.push(arrayImpuestos[i]);
            arrayImpuestos.splice(i, 1);
            long--;
          } else {
            i++;
          }
        }

      } else { // SI HAY DATA DEL PERIODO ANTERIOR, SE ANALIZA

        var long = arrayImpuestos.length;
        var i = 0;
        while (i < long) {
          var long2 = ArrImpMinimos.length;
          var j = 0;
          var seEliminoImp = false;
          var acumular = false;
          while (j < long2) {
            if (arrayImpuestos[i][0] == ArrImpMinimos[j][0] &&
              arrayImpuestos[i][1] == ArrImpMinimos[j][1] &&
              arrayImpuestos[i][2] == ArrImpMinimos[j][2]) {

              if (arrayImpuestos[i][0] == '03') { //Si es IPI, se verifica que sea de la misma subsi
                if (arrayImpuestos[i][4] == ArrImpMinimos[j][4]) {
                  var monto_nuevo = (Number(arrayImpuestos[i][3]) + Number(ArrImpMinimos[j][3])).toFixed(2);
                  acumular = true;
                }
              } else {
                var monto_nuevo = (Number(arrayImpuestos[i][3]) + Number(ArrImpMinimos[j][3])).toFixed(2);
                /*log.debug('monto actual', Number(arrayImpuestos[i][3]));
                log.debug('monto pasado', Number(ArrImpMinimos[j][3]));
                log.debug('acumulado', Number(monto_nuevo));*/
                acumular = true;
              }

              if (acumular) {
                if (monto_nuevo < Monto_min_imp) {
                  ArrImpMinimos[j][3] = monto_nuevo;
                  arrayImpuestos.splice(i, 1);
                  seEliminoImp = true;
                } else {
                  arrayImpuestos[i][3] = monto_nuevo;
                  ArrImpMinimos.splice(j, 1);
                }
                break;
              }

            }

            if (j == long2 - 1) { //si hasta el ultimo no encontró monto anterior
              if (arrayImpuestos[i][3] < Monto_min_imp) {
                ArrImpMinimos.push(arrayImpuestos[i]);
                arrayImpuestos.splice(i, 1);
                seEliminoImp = true;
              }
            }

            j++;
          }

          if (!seEliminoImp) {
            i++;
          } else {
            long--;
          }

        }
      }

      log.debug('impuestos minimos ', ArrImpMinimos);
      return arrayImpuestos;
    }

    function actualizarCreditFileRecord() {
      if (ArrImpMinimos.length != 0) {
        var idfile = guardarImpuestosMinimos(ArrImpMinimos);
        if (InternalidRecordActual == '') {
          var recordCreditFile = recordModulo.create({
            type: 'customrecord_lmry_br_credit_file'
          });
          recordCreditFile.setValue({
            fieldId: 'custrecord_lmry_br_credit_subsi',
            value: param_Subsi
          });
          recordCreditFile.setValue({
            fieldId: 'custrecord_lmry_br_credit_periodo',
            value: (mes_date + anio_date)
          });
          recordCreditFile.setValue({
            fieldId: 'custrecord_lmry_br_credit_feature',
            value: param_Feature
          });
          recordCreditFile.setValue({
            fieldId: 'custrecord_lmry_br_credit_idfile',
            value: idfile
          });

          var recordId = recordCreditFile.save();
        }
      }
    }

    function guardarImpuestosMinimos(arrayData) {
      var strArchivo = '';
      var idFile = '';

      for (var i = 0; i < arrayData.length; i++) {
        strArchivo += arrayData[i].join('|');
        if (i != arrayData.length - 1) {
          strArchivo += '\r\n';
        }
      }

      var folderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_br'
      });

      if (folderId != '' && folderId != null) {
        var fileName = param_Subsi + '_' + mes_date + anio_date + '_' + 'DCTF.txt';
        // Crea el archivo
        var file_aux = fileModulo.create({
          name: fileName,
          fileType: fileModulo.Type.PLAINTEXT,
          contents: strArchivo,
          folder: folderId
        });

        idFile = file_aux.save(); // Termina de grabar el archivo
      } else {
        log.debug('Alerta al guardar impuestos minimos', 'No se encontró folder');
        idFile = ''; // Termina de grabar el archivo
      }
      return idFile;
    }

    function acumularIRRF(arreglo) {
      var monto = 0;
      var arrayAct = [];
      var arrAuxiliar = new Array();
      for (var i = 0; i < arreglo.length; i++) {
        if (arreglo[i][0] == '02') {
          monto += arreglo[i][4];
        }
        if (i == arreglo.length - 1) {
          arrAuxiliar[0] = id_tribute_wht_irrf_repres;
          arrAuxiliar[1] = "IRRF";
          arrAuxiliar[2] = "";
          arrAuxiliar[3] = id_receita_wht_irrf_repres;
          arrAuxiliar[4] = monto;
          arrAuxiliar[5] = periodicidad_irrf_repres;
          arrayAct.push(arrAuxiliar);
        }
      }
      return arrayAct;
    }

    function CargarJournalR11(concepto) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var matrizData = [];

      var savedsearch = search.load({
        /* LatamReady BR - Tax Payment DCTF */
        id: 'customsearch_lmry_br_dctf_tax_payment'
      });
      //Periodo
      var formulaPeriod = "CASE WHEN {custbody_lmry_cl_period.custrecord_lmry_cl_period_fact_actual.id}='" + param_Periodo + "' THEN 1 ELSE 0 END";
      log.debug('formulaPeriod', formulaPeriod);
      var periodFilter = search.createFilter({
        name: 'formulanumeric',
        formula: formulaPeriod,
        operator: search.Operator.EQUALTO,
        values: 1
      });
      savedsearch.filters.push(periodFilter);
      //Concepto
      var formula = "CASE WHEN {custbody_lmry_type_concept.id}='" + concepto + "' THEN 1 ELSE 0 END";
      var conceptFilter = search.createFilter({
        name: 'formulatext',
        operator: search.Operator.IS,
        formula: formula,
        values: 1
      });
      savedsearch.filters.push(conceptFilter);
      //Subsi
      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.ANYOF,
          values: SubsidiariasContempladas
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      if (feature_Multi) {
        var multibook_Filter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch.filters.push(multibook_Filter);
        //15 Debitamount Multibook
        var exchangerate_Column = search.createColumn({
          name: "formulacurrency",
          summary: "SUM",
          formula: "nvl({accountingtransaction.debitamount},0)",
          label: "Formula (Currency)"
        });
        savedsearch.columns.push(exchangerate_Column);
      }
      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
        if (objResult != null) {
          var intLength = objResult.length;

          if (intLength != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            //0. ID DE JOURNAL
            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            //1. FECHA DE VENCIMIENTO
            arrAuxiliar[1] = objResult[i].getValue(columns[5]);
            //2. CODIGO DE TRIBUTO
            arrAuxiliar[2] = objResult[i].getValue(columns[7]);
            //3. CODIGO DE RECETA
            arrAuxiliar[3] = objResult[i].getValue(columns[8]);
            //4. PERIODICIDAD
            arrAuxiliar[4] = objResult[i].getValue(columns[9]);
            //5. AMOUNT- JOUNAL
            if (feature_Multi) {
              arrAuxiliar[5] = objResult[i].getValue(columns[15]);
            } else {
              arrAuxiliar[5] = objResult[i].getValue(columns[10]);
            }
            //6. CNPJ SUBSIDIARIA
            var cnpjSubsi = ValidaGuion(objResult[i].getValue(columns[11]));
            arrAuxiliar[6] = cnpjSubsi;
            //7. AMOUNT CLASS
            var amountClass = objResult[i].getValue(columns[12]);
            arrAuxiliar[7] = amountClass;
            //8. DOCUMENT NUMBER REF.
            var numberRef = objResult[i].getValue(columns[13]);
            arrAuxiliar[8] = numberRef;
            //9. TIPO DE CONCEPTO
            var conceptoTrnsac = objResult[i].getValue(columns[3]);
            arrAuxiliar[9] = conceptoTrnsac;
            //10. PERIODICIDAD FACTOR (POR AHORA SOLO PARA IMPOSTO NOMINA)
            if (concepto == 25) {
              var periodic = objResult[i].getValue(columns[14]);
              arrAuxiliar[10] = periodic;
            }

            if (arrAuxiliar[5] != 0 && conceptoTrnsac == concepto) {
              if (arrAuxiliar[2] == '04' || arrAuxiliar[2] == '09') { //si es impuesto importacion (IOF  o CIDE)
                if (concepto == 5) {
                  if (verificarImportacion(arrAuxiliar[0])) { //si el bill de importacion es configurado correctamente
                    matrizData.push(arrAuxiliar);
                  }
                } else {
                  matrizData.push(arrAuxiliar);
                }
              } else {
                matrizData.push(arrAuxiliar);
              }
            }
          }
          log.error('Lineas Journal de Concepto: ' + concepto, matrizData);
          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
        } else {
          DbolStop = true;
        }
      }
      return matrizData;
    }

    function CargarJournals(registro) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var arrAuxiliar = new Array();
      var dataJournal = [];

      var savedsearch = search.load({
        /* LatamReady BR - Journal R12/R14 */
        id: 'customsearch_lmry_br_dctf_r12_r14'
      });
      //Periodo
      var periodFilter = search.createFilter({
        name: 'postingperiod',
        operator: search.Operator.IS,
        values: [param_Periodo]
      });
      savedsearch.filters.push(periodFilter);
      //Subsidiaria
      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.ANYOF,
          values: SubsidiariasContempladas
        });
        savedsearch.filters.push(subsidiaryFilter);
      }
      //Type Concept
      var concepto = '';
      if (registro == 'R12') {
        concepto = 22;
      } else if (registro == 'R14') {
        concepto = 21;
      }
      var formula = "CASE WHEN {custbody_lmry_type_concept.id}='" + concepto + "' THEN 1 ELSE 0 END";
      log.debug('Formula journals', formula);
      var conceptFilter = search.createFilter({
        name: 'formulatext',
        operator: search.Operator.IS,
        formula: formula,
        values: 1
      });
      savedsearch.filters.push(conceptFilter);

      if (feature_Multi) {
        var multibook_Filter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch.filters.push(multibook_Filter);
        //13 Debitamount Multibook
        var exchangerate_Column = search.createColumn({
          name: "formulacurrency",
          summary: "SUM",
          formula: "nvl({accountingtransaction.debitamount},0) ",
          label: "Monto Debit Multibook"
        });
        savedsearch.columns.push(exchangerate_Column);
      }
      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
        if (objResult != null) {
          var intLength = objResult.length;

          if (intLength != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            arrAuxiliar = new Array();
            //0. ID DE JOURNAL
            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            //1. CODIGO DE TRIBUTO
            arrAuxiliar[1] = objResult[i].getValue(columns[4]);
            //2. CODIGO DE RECETA
            arrAuxiliar[2] = objResult[i].getValue(columns[5]);
            //3. PERIODICIDAD
            arrAuxiliar[3] = objResult[i].getValue(columns[6]);
            //4. DEBIT AMOUNT
            if (feature_Multi) {
              arrAuxiliar[4] = objResult[i].getValue(columns[13]);
            } else {
              arrAuxiliar[4] = objResult[i].getValue(columns[7]);
            }
            //5. ORDEN ESTABLECIMIENTO
            if (arrAuxiliar[1] == '03' || arrAuxiliar[1] == '09') {
              var cnpjSubsi = ValidaGuion(objResult[i].getValue(columns[9]));
              var orden_establecimiento = cnpjSubsi.substr(cnpjSubsi.length - 6, cnpjSubsi.length - 1);
              arrAuxiliar[5] = orden_establecimiento;
            } else {
              arrAuxiliar[5] = '000000';
            }

            if (arrAuxiliar[4] != 0) {

              if (registro == 'R12') {
                //6. Document Number Ref.
                arrAuxiliar[6] = objResult[i].getValue(columns[8]);
              } else {
                //6. JSON R14
                var jsonR14 = objResult[i].getValue(columns[10]);
                if (jsonR14 != null && jsonR14 != '' && jsonR14 != '- None -') {
                  log.debug('jsonR14', jsonR14);
                  arrAuxiliar[6] = JSON.parse(jsonR14);
                } else {
                  arrAuxiliar[6] = '';
                }
                //7. Fecha Vencimiento
                arrAuxiliar[7] = objResult[i].getValue(columns[11]);
                //8. Amount Class
                arrAuxiliar[8] = objResult[i].getValue(columns[12]);
              }
              dataJournal.push(arrAuxiliar);

            }

          }
          log.error('Journals ' + registro, dataJournal);
          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
        } else {
          DbolStop = true;
        }
      }

      return dataJournal;
    }

    function CargarLineasR12_R14(arrayData, tiporegistro) {
      var lineasGeneral = [];
      for (var i = 0; i < arrayData.length; i++) {
        var lineatemp = [];
        //0. Tipo
        lineatemp[0] = tiporegistro;
        //1. CNPJ do Contribuinte
        if (CNPJ != '' && CNPJ != null) {
          lineatemp[1] = ValidaGuion(CNPJ);
        } else {
          lineatemp[1] = completar(14, '', ' ', false);
        }
        //2. MOFG - Mês de Ocorrência do Fato Gerador
        lineatemp[2] = anio_date + mes_date;
        //3. Situação
        if (CNPJ != '' && CNPJ != null) {
          lineatemp[3] = cod_cadastro_Situacion; /////********Setup
        } else {
          lineatemp[3] = '0';
        }
        //4. Data do Evento
        if (lineatemp[3] == '0') {
          lineatemp[4] = '00000000';
        } else {
          lineatemp[4] = anio_date + '' + mes_date + '' + dia_actual;
        }
        //5. COD DEL TRIBUTO
        if (arrayData[i][1] != '' && arrayData[i][1] != null && arrayData[i][1] != '- None -') {
          lineatemp[5] = arrayData[i][1];
        } else {
          lineatemp[5] = '00';
        }
        //6. COD DE LA RECETA
        if (arrayData[i][2] != null && arrayData[i][2] != '' && arrayData[i][2] != '- None -') {
          lineatemp[6] = ValidaGuion(arrayData[i][2]);
        } else {
          lineatemp[6] = '000000';
        }
        //7. PERIODICIDAD
        if (arrayData[i][3] != null && arrayData[i][3] != '' && arrayData[i][3] != '- None -') {
          lineatemp[7] = arrayData[i][3];
        } else {
          lineatemp[7] = completar(1, '', ' ', false);
        }
        //8. Ano do Período de Apuração
        lineatemp[8] = anio_date;
        //9. Mês/Bimestre/Trimestre/Quadrimestre/Semestre do Período de Apuração
        lineatemp[9] = mes_date;
        //10. Dia/Semana/Quinzena/Decêndio do Período de Apuração
        lineatemp[10] = '00';
        //11. Ordem do Estabelecimento
        lineatemp[11] = arrayData[i][5];
        //12. CNPJ da Incorporação/Matrícula CEI
        lineatemp[12] = '00000000000000';
        //13. Reservado
        lineatemp[13] = '0';

        var montoDebit = Number(arrayData[i][4]);
        if (tiporegistro == 'R12') {
          //14. Valor Compensado/Suspensao do Debito
          lineatemp[14] = completar(14, ValidaGuion((montoDebit).toFixed(2)), '0', true);
          //15. Formalização do Pedido
          lineatemp[15] = '3';
          //16. Número da DComp ou Processo
          if (arrayData[i][6] == null || arrayData[i][6] == '' || arrayData[i][6] == '- None -') {
            lineatemp[16] = completar(24, '', '0', true);
          } else {
            lineatemp[16] = completar(24, ValidaGuion(arrayData[i][6]), '0', true);
          }
        } else {
          //14. Valor Compensado/Suspensao do Debito
          lineatemp[14] = montoDebit;

          var jsonR14 = arrayData[i][6];
          //15. Motivo da Suspensão
          jsonR14 == '' ? lineatemp[15] = '' : lineatemp[15] = jsonR14.motSuspensao;
          lineatemp[15] = completar(2, lineatemp[15], ' ', false);
          //16. Depósito
          jsonR14 == '' ? lineatemp[16] = ' ' : lineatemp[16] = jsonR14.deposito;
          lineatemp[16] = completar(1, lineatemp[16], ' ', false);
          //17. Reservado
          lineatemp[17] = '0';
          //18. Número do Processo
          jsonR14 == '' ? lineatemp[18] = '' : lineatemp[18] = jsonR14.nroProceso;
          lineatemp[18] = ValidaGuion(lineatemp[18]);
          lineatemp[18] = completar(24, lineatemp[18], ' ', false); //la data debe tener 20 caracteres
          //19. Vara
          jsonR14 == '' ? lineatemp[19] = ' ' : lineatemp[19] = jsonR14.vara;
          lineatemp[19] = completar(2, lineatemp[19], ' ', false);
          //20. Municipio
          var muni = validarCaracteres_Especiales(city);
          lineatemp[15] == '07' ? lineatemp[20] = '' : lineatemp[20] = muni;
          lineatemp[20] = completar(50, lineatemp[20], ' ', false);
          //21. UF
          lineatemp[15] == '07' ? lineatemp[21] = '' : lineatemp[21] = uf_subsi;
          lineatemp[21] = completar(2, lineatemp[21], ' ', false);
          //22. Identificação do Depósito
          jsonR14 == '' ? lineatemp[22] = '' : lineatemp[22] = jsonR14.identDeposito;
          lineatemp[22] = ValidaGuion(lineatemp[22]);
          lineatemp[22] = completar(20, lineatemp[22], ' ', false); //la data debe tener 16 caracteres
          //23. Período de Apuração
          lineatemp[16] == '1' ? lineatemp[23] = periodenddate : lineatemp[23] = '00000000';
          //24. CPF/CNPJ
          lineatemp[16] == '1' ? lineatemp[24] = companyruc : lineatemp[24] = '              ';
          //25. Codigo de Receita
          lineatemp[16] == '1' ? lineatemp[25] = jsonR14.codReceita : lineatemp[25] = '0000';
          //26. Data de Vencimiento
          lineatemp[16] == '1' ? lineatemp[26] = arrayData[i][7] : lineatemp[26] = '00000000';

          var classAmount = arrayData[i][8];
          //27. Valor do principal
          (classAmount == '' || classAmount == null || classAmount == '1') && lineatemp[16] == '1' ? lineatemp[27] = montoDebit : lineatemp[27] = 0;
          //28. Valor de Multa
          classAmount == '2' && lineatemp[16] == '1' ? lineatemp[28] = montoDebit : lineatemp[28] = 0;
          //29. Valor de Juros
          classAmount == '3' && lineatemp[16] == '1' ? lineatemp[29] = montoDebit : lineatemp[29] = 0;
        }
        /* 17/30 Reservado */
        lineatemp.push('          ');
        /* 18/31 Delimitador de Registro */
        lineatemp.push('\r\n');

        if (tiporegistro == 'R14') {
          /* 32 INTERNAL ID */
          lineatemp.push(arrayData[i][0]);
        }

        lineasGeneral.push(lineatemp);
      }
      log.error('Lineas Journal ' + tiporegistro, lineasGeneral);
      return lineasGeneral;
    }

    function agruparR14(arrayData) {
      var arrActualizado = [];
      var long = arrayData.length;
      var i = 0;
      var j = 0;

      while (j < long) {
        var montoSusp = Number(arrayData[j][14]);
        var montoPrincipal = Number(arrayData[j][27]);
        var montoMulta = Number(arrayData[j][28]);
        var montoJuros = Number(arrayData[j][29]);

        i = j + 1;
        while (i < long) { //internalid-tributo-receta
          if (arrayData[j][32] == arrayData[i][32] && arrayData[j][5] == arrayData[i][5] && arrayData[j][6] == arrayData[i][6]) {
            montoSusp += Number(arrayData[i][14]);
            montoPrincipal += Number(arrayData[i][27]);
            montoMulta += Number(arrayData[i][28]);
            montoJuros += Number(arrayData[i][29]);
            arrayData.splice(i, 1);
            long--;
          } else {
            i++;
          }
        }
        arrayData[j][14] = completar(14, ValidaGuion((montoSusp).toFixed(2)), '0', true);
        arrayData[j][27] = completar(14, ValidaGuion((montoPrincipal).toFixed(2)), '0', true);
        arrayData[j][28] = completar(14, ValidaGuion((montoMulta).toFixed(2)), '0', true);
        arrayData[j][29] = completar(14, ValidaGuion((montoJuros).toFixed(2)), '0', true);
        arrayData[j].pop();
        arrActualizado.push(arrayData[j]);
        j++;
      }
      log.debug('arrActualizado R14', arrActualizado);
      return arrActualizado;
    }

    function verificarImportacion(idJournal) {
      var resultado = false;
      for (var i = 0; i < ArrTempJournalImportacion.length; i++) {
        if (ArrTempJournalImportacion[i][7] == idJournal) {
          resultado = true;
          break;
        }
      }

      return resultado;
    }

    function ObtenerSubRegistro(tributo, receita, periodicidad, ordenEstablec, arrayData) {
      var filasSubregistro = '';
      var long = arrayData.length;
      var i = 0;

      while (i < long) {
        if (arrayData[i][5] == tributo && arrayData[i][6] == receita && arrayData[i][7] == periodicidad && arrayData[i][11] == ordenEstablec) {
          //log.error('entro r11', tributo + '|' + receita + '|' + periodicidad);
          filasSubregistro += arrayData[i].join('');
          cont_reg++;
          arrayData.splice(i, 1);
          long--;
        } else {
          i++;
        }
      }
      return filasSubregistro;
    }

    function formatLinesR11(arrayData) {
      for (var i = 0; i < arrayData.length; i++) {
        r11Columna = [];
        //0. Tipo
        r11Columna[0] = 'R11';
        //1. CNPJ do Contribuinte
        if (CNPJ != '' && CNPJ != null) {
          r11Columna[1] = ValidaGuion(CNPJ);
        } else {
          r11Columna[1] = completar(14, '', ' ', false);
        }
        //2. MOFG - Mês de Ocorrência do Fato Gerador
        r11Columna[2] = anio_date + mes_date;
        //3. Situação
        r11Columna[3] = '0';
        if (CNPJ != '' && CNPJ != null) {
          r11Columna[3] = cod_cadastro_Situacion; /////********Setup
        }
        //4. Data do Evento
        if (r11Columna[3] == '0') {
          r11Columna[4] = '00000000';
        } else {
          r11Columna[4] = anio_date + '' + mes_date + '' + dia_actual;
        }
        //5. COD DEL TRIBUTO
        r11Columna[5] = arrayData[i][0];
        //6. COD DE LA RECETA
        r11Columna[6] = ValidaGuion(arrayData[i][2]);
        //7. PERIODICIDAD
        r11Columna[7] = arrayData[i][3];
        //8. Ano do Período de Apuração
        r11Columna[8] = anio_date;
        //9. Mês/Bimestre/Trimestre/Quadrimestre/Semestre do Período de Apuração
        r11Columna[9] = mes_date;
        //10. Dia/Semana/Quinzena/Decêndio do Período de Apuração
        r11Columna[10] = '00';
        //11. Ordem do Estabelecimento
        if (r11Columna[5] == '03' || r11Columna[5] == '09') { //IPI y CIDE
          var cnpjSubsi = ValidaGuion(arrayData[i][8]);
          var long_cnpj = cnpjSubsi.length;
          var orden_establecimiento = cnpjSubsi.substr(long_cnpj - 6, long_cnpj - 1);
          r11Columna[11] = orden_establecimiento;
        } else {
          r11Columna[11] = '000000';
        }
        //12. CNPJ da Incorporação/Matrícula CEI
        r11Columna[12] = '00000000000000';
        //13. Reservado
        r11Columna[13] = '0';
        //14. Período de Apuração
        r11Columna[14] = dia_date + mes_date + anio_date;
        //15. CNPJ do DARF
        r11Columna[15] = ValidaGuion(arrayData[i][8]);
        //16. Código da Receita do DARF
        r11Columna[16] = '0000';
        if (r11Columna[11] != null && r11Columna[11] != '') {
          r11Columna[16] = completar(4, r11Columna[11], '0', true);
        }
        //17. Data de Vencimento
        r11Columna[17] = '        ';
        //log.debug('vencimiento', arrayData[i][1]);
        if (arrayData[i][10] != null && arrayData[i][10] != '' && arrayData[i][10] != '- None -') {
          r11Columna[17] = ValidaGuion(arrayData[i][10]);
        }
        //18. Nº de Referência
        r11Columna[18] = completar(17, '', ' ', false);
        //19. Valor do Principal
        var principal = ValidaGuion(Number(arrayData[i][4]).toFixed(2));
        r11Columna[19] = completar(14, principal, '0',true) ;
        //20. Valor da Multa
        var multa = ValidaGuion(Number(arrayData[i][14]).toFixed(2));
        r11Columna[20] = completar(14, multa, '0',true) ;
        //21. Valor dos Juros
        var juros = ValidaGuion(Number(arrayData[i][13]).toFixed(2));
        r11Columna[21] = completar(14, juros, '0',true) ;
        //22. Valor pago do Débito (sumatoria de 19+20+21)
        var montoTotal = (redondear(arrayData[i][4]) + redondear(arrayData[i][14]) + redondear(arrayData[i][13])).toFixed(2);
        montoTotal = ValidaGuion(montoTotal);
        r11Columna[22] = completar(14, montoTotal, '0',true) ;
        //23. Reservado
        r11Columna[23] = completar(10, '', ' ', false);
        //24. Delimitador de registro
        r11Columna[24] = '\r\n';

        if (arrayData[i][9] == 5) {
          EPayLinesPago.push(r11Columna);
        } else {
          EPayLinesNomina.push(r11Columna);
        }
      }
    }

    function CargarR11(arrTotal) {
      for (var i = 0; i < arrTotal.length; i++) {
        r11Columna = [];
        //0. Tipo
        r11Columna[0] = 'R11';
        //1. CNPJ do Contribuinte
        if (CNPJ != '' && CNPJ != null) {
          r11Columna[1] = ValidaGuion(CNPJ);
        } else {
          r11Columna[1] = completar(14, '', ' ', false);
        }
        //2. MOFG - Mês de Ocorrência do Fato Gerador
        r11Columna[2] = anio_date + mes_date;
        //3. Situação
        r11Columna[3] = '0';
        if (CNPJ != '' && CNPJ != null) {
          r11Columna[3] = cod_cadastro_Situacion; /////********Setup
        }
        //4. Data do Evento
        if (r11Columna[3] == '0') {
          r11Columna[4] = '00000000';
        } else {
          r11Columna[4] = anio_date + '' + mes_date + '' + dia_actual;
        }
        //5. COD DEL TRIBUTO
        r11Columna[5] = '00';
        if (arrTotal[i][2] != '' && arrTotal[i][2] != null && arrTotal[i][2] != '- None -') {
          r11Columna[5] = arrTotal[i][2];
        }
        //6. COD DE LA RECETA
        r11Columna[6] = '000000';
        if (arrTotal[i][3] != null && arrTotal[i][3] != '' && arrTotal[i][3] != '- None -') {
          r11Columna[6] = ValidaGuion(arrTotal[i][3]);
        }
        //7. PERIODICIDAD
        r11Columna[7] = ' ';
        if (arrTotal[i][4] != null && arrTotal[i][4] != '' && arrTotal[i][4] != '- None -') {
          r11Columna[7] = arrTotal[i][4];
        }

        if (arrTotal[i][9] == 25) {
          var factorPeriodic = completar(2, arrTotal[i][10], '0', true);
          var jsonData = definirPeriodoApuracao(r11Columna[7], factorPeriodic);
          //8. Ano do Período de Apuração
          r11Columna[8] = jsonData.campo9;
          //9. Mês/Bimestre/Trimestre/Quadrimestre/Semestre do Período de Apuração
          r11Columna[9] = jsonData.campo10;
          //10. Dia/Semana/Quinzena/Decêndio do Período de Apuração
          r11Columna[10] = jsonData.campo11;
        } else {
          //8. Ano do Período de Apuração
          r11Columna[8] = anio_date;
          //9. Mês/Bimestre/Trimestre/Quadrimestre/Semestre do Período de Apuração
          r11Columna[9] = mes_date;
          //10. Dia/Semana/Quinzena/Decêndio do Período de Apuração
          r11Columna[10] = '00';
        }

        //11. Ordem do Estabelecimento
        if (r11Columna[5] == '03' || r11Columna[5] == '09') { //IPI y CIDE
          var cnpjSubsi = arrTotal[i][6];
          var long_cnpj = cnpjSubsi.length;
          var orden_establecimiento = cnpjSubsi.substr(long_cnpj - 6, long_cnpj - 1);
          r11Columna[11] = orden_establecimiento;
        } else {
          r11Columna[11] = '000000';
        }
        //12. CNPJ da Incorporação/Matrícula CEI
        r11Columna[12] = '00000000000000';
        //13. Reservado
        r11Columna[13] = '0';
        //14. Período de Apuração
        r11Columna[14] = dia_date + mes_date + anio_date;
        //15. CNPJ do DARF
        r11Columna[15] = arrTotal[i][6];
        //16. Código da Receita do DARF
        r11Columna[16] = '0000';
        if (r11Columna[6] != null && r11Columna[6] != '') {
          r11Columna[16] = completar(4, r11Columna[6], '0', true);
        }
        //17. Data de Vencimento
        r11Columna[17] = '        ';
        //log.debug('vencimiento', arrTotal[i][1]);
        if (arrTotal[i][1] != null && arrTotal[i][1] != '' && arrTotal[i][1] != '- None -') {
          r11Columna[17] = ValidaGuion(arrTotal[i][1]);
        }
        //18. Nº de Referência
        r11Columna[18] = completar(17, '', ' ', false);
        if (arrTotal[i][8] != null && arrTotal[i][8] != '- None -' && arrTotal[i][8] != '') {
          r11Columna[18] = completar(17, arrTotal[i][8], ' ', false);
        }

        /* AMOUNT CLASS */
        var amountClass = arrTotal[i][7];
        var montoDebit = Number(arrTotal[i][5]);
        //19. Valor do Principal
        (amountClass == null || amountClass == '' || amountClass == '1') ? r11Columna[19] = montoDebit: r11Columna[19] = 0;
        //20. Valor da Multa
        amountClass == '2' ? r11Columna[20] = montoDebit : r11Columna[20] = 0;
        //21. Valor dos Juros
        amountClass == '3' ? r11Columna[21] = montoDebit : r11Columna[21] = 0;
        //22. Valor pago do Débito (sumatoria de 19+20+21)
        r11Columna[22] = r11Columna[19] + r11Columna[20] + r11Columna[21];
        //23. Reservado
        r11Columna[23] = completar(10, '', ' ', false);
        //24. Delimitador de registro
        r11Columna[24] = '\r\n';
        //25. INTERNAL ID
        r11Columna[25] = arrTotal[i][0];

        if (arrTotal[i][9] == 5) {
          ArrJournalR11.push(r11Columna);
        } else {
          ArrR11Nomina.push(r11Columna);
        }

      }
      log.error('ArrJournalR11 generado total', ArrJournalR11);
      log.error('ArrR11Nomina generado total', ArrR11Nomina);

      return ArrJournalR11;
    }

    function agruparPagos(arrayData) {
      var arrActualizado = [];
      var long = arrayData.length;
      var i = 0;
      var j = 0;

      while (j < long) {
        var montoPrincipal = Number(arrayData[j][19]);
        var montoMulta = Number(arrayData[j][20]);
        var montoJuros = Number(arrayData[j][21]);
        var montoTotal = Number(arrayData[j][22]);

        i = j + 1;
        while (i < long) { //internalid - tributo - receta - periodicidad_campo11
          if (arrayData[j][25] == arrayData[i][25] && arrayData[j][5] == arrayData[i][5] && arrayData[j][6] == arrayData[i][6] && arrayData[j][10] == arrayData[i][10]) {
            montoPrincipal += Number(arrayData[i][19]);
            montoMulta += Number(arrayData[i][20]);
            montoJuros += Number(arrayData[i][21]);
            montoTotal += Number(arrayData[i][22]);
            arrayData.splice(i, 1);
            long--;
          } else {
            i++;
          }
        }
        arrayData[j][19] = completar(14, ValidaGuion((montoPrincipal).toFixed(2)), '0', true);
        arrayData[j][20] = completar(14, ValidaGuion((montoMulta).toFixed(2)), '0', true);
        arrayData[j][21] = completar(14, ValidaGuion((montoJuros).toFixed(2)), '0', true);
        arrayData[j][22] = completar(14, ValidaGuion((montoTotal).toFixed(2)), '0', true);
        arrayData[j].pop(); //se saca el internal id, y deja los elementos de la linea r11 exactos
        arrActualizado.push(arrayData[j]);
        j++;
      }
      log.debug('arrActualizado R11', arrActualizado);
      return arrActualizado;
    }

    function GenerarArreglos() {
      param_archivo = Number(param_archivo);
      var arrAuxiliar;
      var string = fileModulo.load({
        id: param_archivo
      });
      var contenido = string.getContents()
      ArreglodeTodo = contenido.split('@');
      arrAuxiliarVentas = ArreglodeTodo[0].split('|');
      arrAuxiliarCompra = ArreglodeTodo[1].split('|');
      arrAuxiliarJournal = ArreglodeTodo[2].split('|');
      var arrAuxiliarBillPayment = ArreglodeTodo[3].split('|');

      for (var i = 0; i < arrAuxiliarVentas.length; i++) {
        if (arrAuxiliarVentas[i] == '') {
          break;
        }
        arrAuxiliar = arrAuxiliarVentas[i].split(';');
        if (arrAuxiliar[5] == 'CustInvc') {
          if (arrAuxiliar[7] == '99') {
            ArrSetupDCTF_Sales.push(arrAuxiliar);
          } else if (arrAuxiliar[7] == '55') {
            ArrSetupDCTF_Sales_Inv.push(arrAuxiliar);
          }
        }
      }

      for (var j = 0; j < arrAuxiliarCompra.length; j++) {
        if (arrAuxiliarCompra[j] == '') {
          break;
        }
        arrAuxiliar = arrAuxiliarCompra[j].split(';');
        if (arrAuxiliar[5] == 'VendBill') {
          if (arrAuxiliar[7] == '99') {
            ArrSetupDCTF_Purchases.push(arrAuxiliar);
          } else if (arrAuxiliar[7] == '55') {
            ArrSetupDCTF_Purchases_Inv.push(arrAuxiliar);
          } else if (arrAuxiliar[7] == 'DF') {
            EPayLines.push(arrAuxiliar); //los que tienen multas y juros
          }
        }
      }

      for (var j = 0; j < arrAuxiliarJournal.length; j++) {
        if (arrAuxiliarJournal[j] == '') {
          break;
        }
        arrAuxiliar = arrAuxiliarJournal[j].split(';');
        if (arrAuxiliar[5] == 'Journal') {
          if (arrAuxiliar[9] == 'm') { //solo journals asociados a proceso de importacion manual se tomaran como r10 y r11.
            if (arrAuxiliar[0] == '09') { //CIDE
              ArrCIDEJournal.push(arrAuxiliar);
            } else if (arrAuxiliar[0] == '04') {
              ArrIOFJournal.push(arrAuxiliar);
            }
          }
          //para validar pagos en el registro r11 en verificarImportacion(idjournal), tanto de lineas asociadas
          //a proceso manual como automatico.
          ArrTempJournalImportacion.push(arrAuxiliar);
        }
      }

      for (var j = 0; j < arrAuxiliarBillPayment.length; j++) {
        if (arrAuxiliarBillPayment[j] == '') {
          break;
        }
        arrAuxiliar = arrAuxiliarBillPayment[j].split(';');
        if (arrAuxiliar[5] == 'VendPymt') {
          if (arrAuxiliar[7] == 'DF') {
            EPayLines.push(arrAuxiliar);
          } else if (arrAuxiliar[0] == '09') { //CIDE
            ArrCIDEBillPay.push(arrAuxiliar);
          } else if (arrAuxiliar[0] == '04') {
            ArrIOFBillPay.push(arrAuxiliar);
          }
        }
      }

      log.error('valor de arreglo de Ventas- Servicios', ArrSetupDCTF_Sales);
      log.error('valor d arrreglo de Ventas - INventario', ArrSetupDCTF_Sales_Inv);
      log.error('valor de arreglo de compras - Servicios', ArrSetupDCTF_Purchases);
      log.error('vaor de arreglo de comrpas - Inventario', ArrSetupDCTF_Purchases_Inv);
      log.error('vaor de arreglo de Journal IOF', ArrIOFJournal);
      log.error('vaor de arreglo de Journal CIDE', ArrCIDEJournal);
      log.error('vaor de arreglo de Bill Pay IOF', ArrIOFBillPay);
      log.error('vaor de arreglo de Bill Pay CIDE', ArrCIDEBillPay);
      log.error('vaor de arreglo de Bill Pay EPayment', EPayLines);
    }

    function separarIPI_filiales() {
      var stringIPI_filiales = '';
      for (var i = 0; i < Filiales.length; i++) {
        var matrizIPIVentas = [];
        var matrizIPICompras = [];

        for (var j = 0; j < ArrSetupDCTF_Sales_Inv.length; j++) {
          if (ArrSetupDCTF_Sales_Inv[j][8] == Filiales[i]) {
            if (ArrSetupDCTF_Sales_Inv[j][1] == 'IPI') {
              matrizIPIVentas.push(ArrSetupDCTF_Sales_Inv[j]);
              ArrSetupDCTF_Sales_Inv.splice(j, 1); //se elimina de la matriz principal
            }
          }
        }
        if (matrizIPIVentas.length != 0) {
          log.debug('Matriz de IPI Ventas para la filial ' + Filiales[i], matrizIPIVentas);
        }
        for (var j = 0; j < ArrSetupDCTF_Purchases_Inv.length; j++) {
          if (ArrSetupDCTF_Purchases_Inv[j][8] == Filiales[i]) {
            if (ArrSetupDCTF_Purchases_Inv[j][1] == 'IPI') {
              matrizIPICompras.push(ArrSetupDCTF_Purchases_Inv[j]);
              ArrSetupDCTF_Purchases_Inv.splice(j, 1); //se elimina de la matriz principal
            }
          }
        }
        if (matrizIPICompras.length != 0) {
          log.debug('Matriz de IPI Compras para la filial ' + Filiales[i], matrizIPICompras);
        }

        if (matrizIPIVentas.length != 0 || matrizIPICompras.length != 0) {
          var subsi_temp = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: Number(Filiales[i]),
            columns: ['taxidnum']
          });
          var cnpjFilial = subsi_temp.taxidnum;

          var imp_actualizados = calcularIPI(matrizIPIVentas, matrizIPICompras, Filiales[i]);
          imp_actualizados = eliminarMontosCeros(imp_actualizados);
          if (Acum_imp_min) {
            imp_actualizados = actualizarMontosMinimos(imp_actualizados);
          } else {
            imp_actualizados = eliminarMinimos(imp_actualizados);
          }
          stringIPI_filiales += armarR10(imp_actualizados, cnpjFilial);

        }
      }

      return stringIPI_filiales;
    }

    function calcularIPI(arrayVentasInv, arrayComprasInv, subsiFilial) {
      var impuestosR10 = [];
      var cod_tributo;
      var cod_receita;
      var periodicidad;
      var impuesto = 0;

      if (arrayComprasInv.length > 0) {
        var cod_tributo = arrayComprasInv[0][0];
        var cod_receita = arrayComprasInv[0][2];
        var periodicidad = arrayComprasInv[0][3];

        if (arrayVentasInv.length > 0) {
          impuesto = Number(Number(arrayVentasInv[0][4]).toFixed(2) - Number(arrayComprasInv[0][4]).toFixed(2)).toFixed(2);
        } else {
          log.debug('impuesto a favor', arrayComprasInv);
          impuesto = -Number(arrayComprasInv[0][4]).toFixed(2);
        }
      } else {
        var cod_tributo = arrayVentasInv[0][0];
        var cod_receita = arrayVentasInv[0][2];
        var periodicidad = arrayVentasInv[0][3];

        impuesto = (Number(arrayVentasInv[0][4])).toFixed(2);
      }

      var arrTemp = [cod_tributo, cod_receita, periodicidad, impuesto, subsiFilial];
      impuestosR10.push(arrTemp);

      log.debug('lineas R10 IPI filiales', impuestosR10);
      return impuestosR10
    }

    function acumularMontosMatriz(matrizGeneral) {
      var matrizActualizada = [];
      var long = matrizGeneral.length;
      var i = 0;

      while (i < long) {
        var montoAcumulado = Number(matrizGeneral[i][4]);
        var j = i + 1;
        while (j < long) { //acumulando por cod. Tributo - receita - periodicidad
          if (matrizGeneral[i][0] == matrizGeneral[j][0] && matrizGeneral[i][2] == matrizGeneral[j][2] && matrizGeneral[i][3] == matrizGeneral[j][3]) {
            montoAcumulado += Number(matrizGeneral[j][4]);
            matrizGeneral.splice(j, 1);
            long--;
          } else {
            j++;
          }
        }
        matrizGeneral[i][4] = (montoAcumulado).toFixed(2);
        matrizActualizada.push(matrizGeneral[i]);
        i++;
      }

      log.debug('array acumulado', matrizActualizada);
      return matrizActualizada;
    }

    function ObtenerWHT() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var comilla = "'";

      var DbolStop = false;
      var arrAuxiliar = new Array();

      var _cont = 0;

      var savedsearch = search.load({
        /* LatamReady - BR WHT DCTF */
        id: 'customsearch_lmry_br_dctf_reten'
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

      if (feature_Multi) {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch.filters.push(multibookFilter);

        //columna 10 -- exchangerate del multiubook
        var exchange_rate_multi = search.createColumn({
          name: "exchangerate",
          join: "accountingtransaction"
        });
        savedsearch.columns.push(exchange_rate_multi);
      }

      //log.error('haber  haber',savedsearch);
      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
        if (objResult != null) {
          var intLength = objResult.length;

          if (intLength != 1000) {
            DbolStop = true;
          }
          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            arrAuxiliar = new Array();
            //1. Id Tributo
            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            //2. Tributo
            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            //3. Receta
            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
            //4. Id Receta
            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            //5. Monto
            var whtLocalCurrency = objResult[i].getValue(columns[9]);
            //log.debug('whtLocalCurrency',whtLocalCurrency);
            if (whtLocalCurrency != null && whtLocalCurrency != 0 && whtLocalCurrency != '- None -') {
              arrAuxiliar[4] = redondear(whtLocalCurrency);
            } else {
              //log.debug('no tiene local currency', arrAuxiliar);
              //para obtener tipo de cambio
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                var exch_rate_nf = objResult[i].getValue(columns[5]);
                exch_rate_nf = exchange_rate(exch_rate_nf);
              } else {
                if (feature_Multi) {
                  var exch_rate_nf = objResult[i].getValue(columns[10]);
                } else {
                  var exch_rate_nf = objResult[i].getValue(columns[8]);
                }
              }

              var whtCalculado = redondear(objResult[i].getValue(columns[4])) * exch_rate_nf;
              arrAuxiliar[4] = redondear(whtCalculado);
            }
            //log.debug('monto retencion',arrAuxiliar[4]);
            //6. Periodicidad
            arrAuxiliar[5] = objResult[i].getValue(columns[6]);

            if (objResult[i].getValue(columns[7]) == id_item_repres_legal) {
              Arr_WHT_Item.push(arrAuxiliar);
            } else {
              Arr_WHT_No_Item.push(arrAuxiliar);
            }
            _cont++;
          }
          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
        } else {
          DbolStop = true;
        }
      }
      log.debug('tamaño del arreglo no item', Arr_WHT_No_Item);
      log.debug('tamaño del arreglo item', Arr_WHT_Item);
    }

    function redondear(number) {
      return Math.round(Number(number) * 100) / 100;
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
      //Periodo
      record.setValue({
        fieldId: 'custrecord_lmry_br_rg_period',
        value: periodname
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


    function SaveFile() {
      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_br'
      });
      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        // Extension del archivo
        var NameFile = Name_File() + '.dec';
        // Crea el archivo
        var file = fileModulo.create({
          name: NameFile,
          fileType: fileModulo.Type.PLAINTEXT,
          contents: strArchivo,
          encoding: fileModulo.Encoding.ISO_8859_1,
          folder: FolderId
        });

        var idfile = file.save(); // Termina de grabar el archivo
        var idfile2 = fileModulo.load({
          id: idfile
        }); // Trae URL de archivo generado

        // Obtenemo de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
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

          var record = recordModulo.load({
            type: 'customrecord_lmry_br_rpt_generator_log',
            id: param_RecorID
          });
          //Periodo
          record.setValue({
            fieldId: 'custrecord_lmry_br_rg_period',
            value: periodname
          });
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

    function Name_File() {
      var name = '';
      if (feature_Multi) {
        name = 'DCTF' + companyruc + '_' + param_Subsi + '_' + mes_date + anio_date + '_' + param_Multi;
      } else {
        name = 'DCTF' + companyruc + '_' + param_Subsi + '_' + mes_date + anio_date;
      }
      return name;
    }

    function ObtnerSetupRptDCTF() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;

      var DbolStop = false;
      var arrAuxiliar = new Array();

      var _cont = 0;

      var savedsearch = search.create({
        type: 'customrecord_lmry_br_setup_rpt_dctf',
        columns: [
          //0
          search.createColumn({
            name: "custrecord_lmry_br_situacao",
            label: "LATAM - BR SITUACAO"
          }),
          //1
          search.createColumn({
            name: "custrecord_lmry_br_code_taxation_profit",
            join: "CUSTRECORD_LMRY_BR_FORMA_TRIBUTACION",
            label: "Latam - BR Forma Tributaria Code"
          }),
          //02 Latam - BR Calificacion Code
          search.createColumn({
            name: "custrecord_lmry_br_code_qualif_pj",
            join: "CUSTRECORD_LMRY_BR_CALIFICACION_PJ",
            label: "Latam - BR Calificacion Code"
          }),
          //03 Latam - BR Balancete de Suspensao PJ
          search.createColumn({
            name: "custrecord_lmry_br_balanceo_suspenso_pj",
            label: "Latam - BR Balancete de Suspensao PJ"
          }),
          //04 Latam - BR PJ Debitos SCP
          search.createColumn({
            name: "custrecord_lmry_br_pj_debito_scp",
            label: "Latam - BR PJ Debitos SCP"
          }),
          //05 Latam - BR PJ Simple Nacional
          search.createColumn({
            name: "custrecord_lmry_br_pj_simple_nacional",
            label: "Latam - BR PJ Simple Nacional"
          }),
          //06 Latam - BR PJ CPRB
          search.createColumn({
            name: "custrecord_lmry_br_pj_cprb",
            label: "Latam - BR PJ CPRB"
          }),
          //07 Latam - BR PJ Inactivo Mes Declarado
          search.createColumn({
            name: "custrecord_lmry_br_pj_inactivo_mes",
            label: "Latam - BR PJ Inactivo Mes Declarado"
          }),
          //08 Latam - BR Criterio Variable Monetaria
          search.createColumn({
            name: "custrecord_lmry_br_code_monetary_variat",
            join: "CUSTRECORD_LMRY_BR_CRITERIO_VARIAB_MONE",
            label: "Latam - BR Criterio Variable Monetaria"
          }),
          //09 Latam - BR Situacion PJ Mes Declarado
          search.createColumn({
            name: "custrecord_lmry_br_code_situation_pj",
            join: "CUSTRECORD_LMRY_BR_SITUACION_PJ_MES_DEC",
            label: "Latam - BR Situacion PJ Mes Declarado"
          }),
          //10 Latam - BR Opcion Ref 2014
          search.createColumn({
            name: "custrecord_lmry_br_code_options_2014",
            join: "CUSTRECORD_LMRY_BR_OPCION_REF_2014",
            label: "Latam - BR Opcion Ref 2014"
          }),
          //11 Latam - BR Legal Representative
          search.createColumn({
            name: "custrecord_lmry_br_legalrepresentative",
            label: "Latam - BR Legal Representative"
          }),
          //12 Latam - BR Responsable's Name
          search.createColumn({
            name: "custrecord_lmry_br_responsable_dctf",
            label: "Latam - BR Responsable's Name"
          }),
          //13 Latam - BR Account Payroll Internal ID
          search.createColumn({
            name: "internalid",
            join: "CUSTRECORD_LMRY_BR_ACCOUNT_PAYROLL",
            label: " Latam - BR Account Payroll Internal ID"
          }),
          //14 LATAM - BR ID TRIBUTE PAYROLL
          search.createColumn({
            name: "custrecord_lmry_br_id_tribute_payroll",
            label: "Latam - BR Id Tribute PayRoll"
          }),
          //15 LATAM - BR ID RECEITA PAYROLL
          search.createColumn({
            name: "custrecord_lmry_br_id_receita_payroll",
            label: "Latam - BR Id Receita PayRoll"
          }),
          //16 LATAM - BR ID PERIODICITY PAYROLL
          search.createColumn({
            name: "custrecord_lmry_br_id_periodicity_payrol",
            label: "Latam - BR Id Periodicity PayRoll"
          }),
          //17 LATAM - BR LIMITE AMOUNT IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_monto_limite_irpj",
            label: "Latam - BR Limit Amount IRPJ"
          }),
          //18 LATAM - BR PORC ALICUOTA IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_porc_alicuota_irpj",
            label: "Latam - BR Porc Alicuota IRPJ"
          }),
          //19 LATAM - BR PORC ADICIONAL IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_porc_adicional_irpj",
            label: "Latam - BR Porc Adicional IRPJ"
          }),
          //20 LATAM - BR PORC ALICUOTA CSLL
          search.createColumn({
            name: "custrecord_lmry_br_porc_alicuota_csll",
            label: "Latam - BR Porc Alicuota CSLL"
          }),
          //21 LATAM - BR ID RECEITA IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_id_receita_irpj",
            label: "Latam - BR Id Receita IRPJ"
          }),
          //22 LATAM - BR ID PERIODICITY IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_id_periodicity_irpj",
            label: "Latam - BR Id Periodicity IRPJ"
          }),
          //23 LATAM - BR ID RECEITA CSLL
          search.createColumn({
            name: "custrecord_lmry_br_id_receita_csll",
            label: "Latam - BR Id Receita CSLL"
          }),
          //24 LATAM - BR ID PERIODICITY CSLL
          search.createColumn({
            name: "custrecord_lmry_br_id_periodicity_csll",
            label: "Latam - BR Id Periodicity CSLL"
          }),
          //25 Latam - BR ID TRIBUTE WHT - IRRF -Representante
          search.createColumn({
            name: "custrecord_lmry_br_id_tribute_legalper",
            label: "Latam - BR ID TRIBUTE WHT - IRRF - Representante"
          }),
          //26 LATAM - BR ID RECEITA WHT - IRRF -Representante
          search.createColumn({
            name: "custrecord_lmry_br_id_receita_legalper",
            label: "LATAM - BR ID RECEITA WHT - IRRF - Representante"
          }),
          //27 LATAM - BR ID PERIODICITY WHT - IRRF -Representante
          search.createColumn({
            name: "custrecord_lmry_br_id_period_lega_person",
            label: "LATAM - BR ID PERIODICITY WHT - IRRF - Representante"
          }),
          //28 LATAM - BR LEGAL REPRESENTATIVE SERVICES
          search.createColumn({
            name: "internalid",
            join: "CUSTRECORD_LMRY_BR_SERV_REPR_LEGAL",
            label: "LATAM - BR LEGAL REPRESENTATIVE SERVICES"
          }),
          //29 Latam - BR Receita - PIS
          search.createColumn({
            name: "custrecord_lmry_br_receita_pis",
            label: "Latam - BR Receita - PIS"
          }),
          //30 Latam - BR Code Receita - PIS
          search.createColumn({
            name: "custrecord_lmry_br_code_receita_pis",
            label: "Latam - BR Code Receita - PIS"
          }),
          //31 Latam - BR Product Code Tax PIS
          search.createColumn({
            name: "custrecord_lmry_br_product_code_tax_pis",
            label: "Latam - BR Product Code Tax PIS"
          }),
          //32 Latam - BR Id Periodicity - PIS
          search.createColumn({
            name: "custrecord_lmry_br_periodicity_pis",
            label: "Latam - BR Id Periodicity - PIS"
          }),
          //33  Latam - BR Receita - COFINS
          search.createColumn({
            name: "custrecord_lmry_br_receita_cofins",
            label: "  Latam - BR Receita - COFINS"
          }),
          //34 Latam - BR Code Receita - COFINS
          search.createColumn({
            name: "custrecord_lmry_br_code_receita_cofins",
            label: "Latam - BR Code Receita - COFINS"
          }),
          //35 Latam - BR Product Code Tax COFINS
          search.createColumn({
            name: "custrecord_lmry_br_product_code_tax_cofi",
            label: "Latam - BR Product Code Tax COFINS"
          }),
          //36 Latam - BR Id Periodicity - COFINS
          search.createColumn({
            name: "custrecord_lmry_br_periodicity_cofins",
            label: "Latam - BR Id Periodicity - COFINS"
          }),
          //37 LATAM - BR RECEITA IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_receta_irpj",
            label: "LATAM - BR RECEITA IRPJ"
          }),
          //38 Latam - BR Receita CSLL
          search.createColumn({
            name: "custrecord_lmry_br_receta_csll",
            label: "Latam - BR Receita CSLL"
          }),
          //39
          search.createColumn({
            name: "custrecord_lmry_br_tipo_concepto_asiento",
            label: "Latam - BR Type Concept Journal"
          }),
          //40 Filiales
          search.createColumn({
            name: "custrecord_lmry_br_filiales",
            label: "Filiales"
          }),
          //41 Acumulacion Imp Minimos
          search.createColumn({
            name: "custrecord_lmry_br_dctf_imp_min",
            label: "Latam - BR Apply Minimum Taxes"
          }),
          //42 Impuesto Minimo
          search.createColumn({
            name: "custrecord_lmry_br_impt_minimo",
            label: "Latam - BR Minimum Taxes"
          }),
          //43 Acumulacion WHT Mínimos
          search.createColumn({
            name: "custrecord_lmry_br_dctf_wht_min",
            label: "Latam - BR Apply Minimum WHT"
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

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;

          if (intLength != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            arrAuxiliar = new Array();

            var codigo_situacao = objResult[i].getValue(columns[0]);

            if (codigo_situacao != null && codigo_situacao != '') {
              var busqeuda_situacion = search.create({
                type: 'customrecord_lmry_br_situation',
                filters: [
                  ['isinactive', 'is', 'F'], "AND", ['internalid', 'is', codigo_situacao]
                ],
                columns: ['custrecord_lmry_br_declaration_code', 'custrecord_lmry_br_cadastro_code']
              });
              var resultado2 = busqeuda_situacion.run().getRange(0, 1000);
              if (resultado2.length != 0 && resultado2 != null) {
                cod_decla_Situacion = resultado2[0].getValue('custrecord_lmry_br_declaration_code');
                cod_cadastro_Situacion = resultado2[0].getValue('custrecord_lmry_br_cadastro_code');
              } else {
                cod_decla_Situacion = completar(2, '', ' ', false);
                cod_cadastro_Situacion = completar(1, '', ' ', false);
              }
            } else {
              cod_decla_Situacion = completar(2, '', ' ', false);
              cod_cadastro_Situacion = completar(1, '', ' ', false);
            }

            cod_form_tribut = objResult[i].getValue(columns[1]);
            cod_califica = objResult[i].getValue(columns[2]);
            balance_suspensao = objResult[i].getValue(columns[3]);
            pj_debitos_scp = objResult[i].getValue(columns[4]);
            pj_simple_nacional = objResult[i].getValue(columns[5]);
            pj_cprb = objResult[i].getValue(columns[6]);
            pj_inactivo_mes = objResult[i].getValue(columns[7]);
            variable_monetaria = objResult[i].getValue(columns[8]);
            pj_mes_declara = objResult[i].getValue(columns[9]);
            opcion_ref = objResult[i].getValue(columns[10]);
            legal_representante = objResult[i].getValue(columns[11]);
            name_responsable = objResult[i].getValue(columns[12]);
            id_account_payroll = objResult[i].getValue(columns[13]);

            if (i == 0) {
              Var_Acount_Payroll = id_account_payroll;
            } else {
              Var_Acount_Payroll = Var_Acount_Payroll + ',' + id_account_payroll;
            }

            id_tributo_payroll = objResult[i].getValue(columns[14]);
            id_receta_payroll = objResult[i].getValue(columns[15]);
            id_periodicidad_payroll = objResult[i].getValue(columns[16]);
            monto_limite = objResult[i].getValue(columns[17]);
            porc_alicuota_irpj = objResult[i].getValue(columns[18]);
            porc_adicional_irpj = objResult[i].getValue(columns[19]);
            porc_alicuota_csll = objResult[i].getValue(columns[20]);
            id_receta_irpj = objResult[i].getValue(columns[21]);
            id_periodicidad_irpj = objResult[i].getValue(columns[22]);
            id_receta_csll = objResult[i].getValue(columns[23]);
            id_periodicidad_csll = objResult[i].getValue(columns[24]);
            id_tribute_wht_irrf_repres = objResult[i].getValue(columns[25]);
            id_receita_wht_irrf_repres = objResult[i].getValue(columns[26]);
            periodicidad_irrf_repres = objResult[i].getValue(columns[27]);
            id_item_repres_legal = objResult[i].getValue(columns[28]);
            //NUEVOS CAMPOS CREADOS DENUEVO XD
            id_receta_pis_inv = objResult[i].getValue(columns[29]);
            code_receta_pis_inv = objResult[i].getValue(columns[30]);
            code_tribute_pis = objResult[i].getValue(columns[31]);
            periodicidad_pis_inv = objResult[i].getValue(columns[32]);
            id_receta_cofins_inv = objResult[i].getValue(columns[33]);
            code_receta_cofins_inv = objResult[i].getValue(columns[34]);
            code_tribute_cofins_inv = objResult[i].getValue(columns[35]);
            periodicidad_cofins_inv = objResult[i].getValue(columns[36]);
            receta_irpj = objResult[i].getValue(columns[37]);
            receta_csll = objResult[i].getValue(columns[38]);
            type_concept_journal = objResult[i].getValue(columns[39]);
            Filiales = objResult[i].getValue(columns[40]);
            Acum_imp_min = objResult[i].getValue(columns[41]);
            Monto_min_imp = Number(objResult[i].getValue(columns[42]));
            Acum_wht_min = objResult[i].getValue(columns[43]);
          }
          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
        } else {
          DbolStop = true;
        }
      }
    }

    function CargarHeader() {
      //Sistema
      var columna1 = 'DCTFM';
      //Reservado
      var columna2 = '';
      columna2 = completar(3, columna2, ' ', false);
      //Reservado
      var columna3 = '';
      columna3 = completar(4, columna3, ' ', false);
      //Anio de competencia de declaración
      var columna4 = anio_date;
      //Reservado
      var columna5 = '1930';
      //Tipo de Declaración
      var columna6 = param_Type_Decla;
      //CNPJ de Contribuyente
      var columna7 = ValidaGuion(CNPJ);
      //Reservado
      var columna8 = '0';
      //Version
      var columna9 = completar(3, version, ' ', true);
      //Nombre Empresarial
      var columna10 = companyname;
      columna10 = completar(60, columna10, ' ', false);
      //UF Domicilio
      var columna11 = uf_domicilio;
      columna11 = completar(2, columna11, ' ', false);
      //Reservado
      var columna12 = '';
      columna12 = completar(10, columna12, '0', true);
      //Reservado
      var columna13 = '0';
      //Situación
      var columna14 = completar(2, cod_decla_Situacion, ' ', false); ///////***************Setup
      //Anio de competencia de declaración
      var columna15 = anio_date;
      //Mes de competencia de declaración
      var columna16 = mes_date;
      //Reservado
      var columna17 = '';
      columna17 = completar(11, columna17, '0', true);
      //Periodo Base Inicial
      var columna18 = dia_ini + mes_ini + anio_ini;
      //Periodo Base final
      if (columna14 == '02') {
        var columna19 = dia_actual + '' + mes_actual + '' + anio_date;
      } else {
        var columna19 = periodenddate;
      }
      //Data de Ocurrencia de Evento
      if (columna14 == '00') {
        var columna20 = '00000000';
      } else {
        var columna20 = dia_actual + '' + mes_actual + '' + anio_date;
      }
      //Reservado
      var columna21 = '0';
      //Reservado
      var columna22 = '0';
      //Reservado
      var columna23 = '';
      columna23 = completar(207, columna23, ' ', false);
      //Reservado
      var columna24 = '';
      columna24 = completar(10, columna24, '0', true);
      //Delimitador
      var columna25 = '\r\n';

      strArchivo = strArchivo + columna1 + '' + columna2 + '' + columna3 + '' + columna4 + '' + columna5 +
        '' + columna6 + '' + columna7 + '' + columna8 + '' + columna9 + '' + columna10 +
        '' + columna11 + '' + columna12 + '' + columna13 + '' + columna14 + '' + columna15 +
        '' + columna16 + '' + columna17 + '' + columna18 + '' + columna19 + '' + columna20 +
        '' + columna21 + '' + columna22 + '' + columna23 + '' + columna24 + '' + columna25;
      cont_reg++;

    }

    function CargarDatosIniciales() {
      //Tipo
      var columna1 = 'R01';
      //CNPJ de Contribuyente
      var columna2 = ValidaGuion(CNPJ);
      //MOFG
      var columna3 = anio_date + '' + mes_date;
      //Situación
      var columna4 = cod_cadastro_Situacion; /////********Setup
      //Data do Evento
      if (columna4 == '0') {
        var columna5 = '00000000';
      } else {
        var columna5 = anio_date + '' + mes_actual + '' + dia_actual;
      }
      //Inicio de Periodo
      var columna6 = dia_ini + mes_ini;
      //Fin de Periodo
      var columna7 = dia_date + mes_date;
      //Declaración de Rectificatoria
      var columna8 = param_Type_Decla;
      //Número de recibo
      if (columna8 == '1') {
        var columna9 = param_Num_Recti;
        columna9 = completar(12, columna9, '0', true);
      } else {
        var columna9 = '';
        columna9 = completar(12, columna9, '0', true);
      }
      //Forma de Tributación
      var columna10 = cod_form_tribut; ////*****Setup
      //Calificación de Persona Juridica
      var columna11 = cod_califica; ////*****Setup
      //PJ levantou
      if (balance_suspensao == 'F' || balance_suspensao == false) {
        var columna12 = '0'; ////*****Setup
      } else {
        var columna12 = '1'; ////*****Setup
      }
      //PJ com débitos
      if (pj_debitos_scp == 'F' || pj_debitos_scp == false) {
        var columna13 = '0'; ////*****Setup
      } else {
        var columna13 = '1'; ////*****Setup
      }
      //PJ optante pelo Simples Nacional
      if (pj_simple_nacional == 'F' || pj_simple_nacional == false) {
        var columna14 = '0'; ////*****Setup
      } else {
        var columna14 = '1'; ////*****Setup
      }
      //PJ optante pela CPRB
      if (pj_cprb == 'F' || pj_cprb == false) {
        var columna15 = '0'; ////*****Setup
      } else {
        var columna15 = '1'; ////*****Setup
      }
      //PJ inativa no mês da declaração
      if (pj_inactivo_mes == 'F' || pj_inactivo_mes == false) {
        var columna16 = '0'; ////*****Setup
      } else {
        var columna16 = '1'; ////*****Setup
      }
      //Criterio de Reconocimiento
      if (variable_monetaria == '') {
        var columna17 = ' '; ////*****Setup
      } else {
        var columna17 = variable_monetaria; ////*****Setup
      }
      //Reservado
      var columna18 = '';
      columna18 = completar(11, columna18, '0', true);
      //Régimen de valoración
      if (reg_pis_confis == '') {
        var columna19 = ' '; ////*****Setup
      } else {
        var columna19 = reg_pis_confis; ////*****Setup
      }
      //Situación de PJ
      if (pj_mes_declara == '') {
        var columna20 = ' '; ////*****Setup
      } else {
        var columna20 = pj_mes_declara; ////*****Setup
      }
      //Opciones referentes
      if (opcion_ref == '') {
        var columna21 = '0'; ////*****Setup
      } else {
        var columna21 = opcion_ref; ////*****Setup
      }
      //Reservado
      var columna22 = '';
      columna22 = completar(10, columna22, ' ', false);
      //Delimitador
      var columna23 = '\r\n';

      strArchivo = strArchivo + '' + columna1 + '' + columna2 + '' + columna3 + '' + columna4 + '' + columna5 +
        '' + columna6 + '' + columna7 + '' + columna8 + '' + columna9 + '' + columna10 +
        '' + columna11 + '' + columna12 + '' + columna13 + '' + columna14 + '' + columna15 +
        '' + columna16 + '' + columna17 + '' + columna18 + '' + columna19 + '' + columna20 +
        '' + columna21 + '' + columna22 + '' + columna23;
      cont_reg++;
    }

    function CargaDatosRegistrales() {
      //Tipo
      var columna1 = 'R02';
      //CNPJ de Contribuyente
      var columna2 = ValidaGuion(CNPJ);
      //MOFG
      var columna3 = anio_date + '' + mes_date;
      //Situación
      var columna4 = cod_cadastro_Situacion; /////********Setup
      //Data do Evento
      if (columna4 == '0') {
        var columna5 = '00000000';
      } else {
        var columna5 = anio_date + '' + mes_date + '' + dia_actual;
      }
      //Nombre Empresarial
      var columna6 = companyname;
      columna6 = completar(115, columna6, ' ', false);
      //Reservado
      var columna7 = '0000';
      //Logradouro
      var columna8 = address;
      columna8 = validarCaracteres_Especiales(address);
      columna8 = completar(40, columna8, ' ', false);
      //Número
      var columna9 = num_dir;
      columna9 = completar(6, columna9, ' ', false);
      //Complemento
      var columna10 = validarCaracteres_Especiales(complemento);
      columna10 = completar(21, columna10, ' ', false);
      //Bairro/Distrito
      var columna11 = validarCaracteres_Especiales('' + barrio);
      columna11 = completar(20, columna11, ' ', false);
      //Municipio
      var columna12 = validarCaracteres_Especiales(city);
      columna12 = completar(50, columna12, ' ', false);
      //UF
      var columna13 = uf_subsi;
      columna13 = completar(2, columna13, ' ', false);
      //CEP
      columna14 = zip;
      columna14 = ValidaGuion(columna14);
      columna14 = completar(8, columna14, ' ', false);
      //DDD do Telefone
      var columna15 = completar(4, ddd_subsi, ' ', false);
      //Telefone
      var columna16 = ValidaGuion(phone);
      columna16 = completar(9, columna16, ' ', false);
      //DDD do Fax
      var columna17 = columna15;
      //Fax
      var columna18 = '';
      columna18 = completar(9, columna18, ' ', false);
      //Caixa Postal
      var columna19 = '';
      columna19 = completar(6, columna19, ' ', false);
      //UF da Caixa Postal
      var columna20 = uf_domicilio;
      //var columna20 = uf_subsi;
      columna20 = completar(2, columna20, ' ', false);
      //CEP da Caixa Postal
      cef = ValidaGuion(cef);
      var columna21 = cef;
      //var columna21 = zip;
      columna21 = completar(8, columna21, ' ', false);
      //Correio Eletrônico
      var columna22 = '' + correo;
      columna22 = completar(40, columna22, ' ', false);
      //Reservado
      var columna23 = '';
      columna23 = completar(10, columna23, ' ', false);
      //Delimitador
      var columna24 = '\r\n';

      strArchivo = strArchivo + '' + columna1 + '' + columna2 + '' + columna3 + '' + columna4 + '' + columna5 +
        '' + columna6 + '' + columna7 + '' + columna8 + '' + columna9 + '' + columna10 +
        '' + columna11 + '' + columna12 + '' + columna13 + '' + columna14 + '' + columna15 +
        '' + columna16 + '' + columna17 + '' + columna18 + '' + columna19 + '' + columna20 +
        '' + columna21 + '' + columna22 + '' + columna23 + '' + columna24;
      cont_reg++;
    }

    function CargaDatosResponsable() {
      //Tipo
      var columna1 = 'R03';
      //CNPJ de Contribuyente
      var columna2 = ValidaGuion(CNPJ);
      //MOFG
      var columna3 = anio_date + '' + mes_date;
      //Situación
      var columna4 = cod_cadastro_Situacion; /////********Setup
      //Data do Evento
      if (columna4 == '0') {
        var columna5 = '00000000';
      } else {
        var columna5 = anio_date + '' + mes_date + '' + dia_actual;
      }

      var id_employee_representante = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: legal_representante,
        columns: ['firstname', 'lastname', 'custentity_lmry_sv_taxpayer_number', 'phone', 'fax', 'email']
      });
      //para los campos del address del Representante Legal
      var id_employee_representante_add = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: legal_representante,
        columns: ['address.custrecord_lmry_addr_city']
      });

      //Nome do Representante
      var columna6 = validarCaracteres_Especiales(id_employee_representante.firstname + ' ' + id_employee_representante.lastname);
      columna6 = completar(60, columna6, ' ', false);
      //CPF - Representante
      var columna7 = id_employee_representante.custentity_lmry_sv_taxpayer_number;
      //log.error('columna7', columna7);
      columna7 = ValidaGuion(columna7);
      columna7 = completar(11, columna7, ' ', false);
      //DDD do Telefone - Representante
      if (id_employee_representante_add['address.custrecord_lmry_addr_city'] != undefined) {
        var cityRepresentante = id_employee_representante_add['address.custrecord_lmry_addr_city'];
      } else {
        var cityRepresentante = '';
      }
      if (cityRepresentante != null && cityRepresentante.length != 0) {
        var idCity = cityRepresentante[0].value;
        ddd_tel_representante = obtenerDDD(idCity);
      } else {
        log.debug('Alerta en CargaDatosResponsable', 'No se configuro ciudad para el representante');
        ddd_tel_representante = '';
      }
      var columna8 = completar(4, ddd_tel_representante, ' ', false);
      log.debug('columna 8 representante', columna8);
      //Telefone - Representante
      var columna9 = id_employee_representante.phone;
      log.error('columna9', columna9);
      columna9 = ValidaGuion(columna9);
      columna9 = completar(9, columna9, ' ', false);
      //Ramal - Representante
      var columna10 = '';
      log.error('columna10', columna10);
      columna10 = completar(5, columna10, ' ', false);
      //DDD do Fax - Representante
      var columna11 = columna8;
      log.debug('columna11', columna11);
      //Fax - Representante
      var columna12 = id_employee_representante.fax;
      log.error('columna12', columna12);
      columna12 = completar(9, ValidaGuion(columna12), ' ', false);
      //Correio Eletrônico - Representante
      var columna13 = id_employee_representante.email;
      log.error('columna13', columna13);
      columna13 = completar(40, columna13, ' ', false);

      log.error('name_responsable', name_responsable);

      var id_employee_responsable = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: name_responsable,
        columns: ['firstname', 'lastname', 'custentity_lmry_sv_taxpayer_number', 'custentity_lmry_br_crc', 'phone', 'fax', 'email']
      });
      //para los campos del address
      var id_employee_responsable_add = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: name_responsable,
        columns: ['address.custrecord_lmry_addr_prov_acronym', 'address.custrecord_lmry_addr_city']
      });
      log.error('id_employee_responsable', id_employee_responsable);

      //Nome do Responsável
      var columna14 = validarCaracteres_Especiales(id_employee_responsable.firstname + ' ' + id_employee_responsable.lastname);
      log.error('columna14', columna14);
      columna14 = completar(60, columna14, ' ', false);

      //CPF - Responsável
      var columna15 = id_employee_responsable.custentity_lmry_sv_taxpayer_number;
      log.error('columna15', columna15);
      columna15 = ValidaGuion(columna15);
      log.error('new columna15', columna15);
      columna15 = completar(11, columna15, ' ', false);
      log.error('newest columna15', columna15);
      //CRC - Responsável
      var columna16 = id_employee_responsable.custentity_lmry_br_crc;
      log.error('columna16', columna16);
      columna16 = ValidaGuion(columna16);
      columna16 = completar(15, columna16, ' ', false);
      //UF do CRC - Responsável
      if (id_employee_responsable_add['address.custrecord_lmry_addr_prov_acronym'] != undefined) {
        var columna17 = id_employee_responsable_add['address.custrecord_lmry_addr_prov_acronym'];
      } else {
        var columna17 = '';
      }
      log.error('columna17', columna17);
      columna17 = completar(2, columna17, ' ', false);
      //DDD do Telefone - Responsável
      if (id_employee_responsable_add['address.custrecord_lmry_addr_city'] != undefined) {
        var cityResponsable = id_employee_responsable_add['address.custrecord_lmry_addr_city'];
      } else {
        var cityResponsable = '';
      }
      if (cityResponsable != null && cityResponsable.length != 0) {
        var idCity = cityResponsable[0].value;
        ddd_tel_responsable = obtenerDDD(idCity);
      } else {
        log.debug('Alerta en CargaDatosResponsable', 'No se configuro ciudad para el responsable');
        ddd_tel_responsable = '';
      }
      var columna18 = completar(4, ddd_tel_responsable, ' ', false);
      //Telefone - Responsável
      var columna19 = id_employee_responsable.phone;
      log.error('columna19', columna19);
      columna19 = ValidaGuion(columna19);
      columna19 = completar(9, columna19, ' ', false);
      //Ramal - Responsável
      var columna20 = '';
      columna20 = completar(5, columna20, ' ', false);
      //DDD do Fax - Responsável
      var columna21 = columna18;
      //Fax - Responsável
      var columna22 = id_employee_responsable.fax;
      columna22 = completar(9, ValidaGuion(columna22), ' ', false);
      //Correio Eletrônico - Responsável
      var columna23 = id_employee_responsable.email;
      columna23 = completar(40, columna23, ' ', false);
      //Reservado
      var columna24 = '';
      columna24 = completar(10, columna24, ' ', false);
      //Delimitador
      var columna25 = '\r\n';

      strArchivo = strArchivo + '' + columna1 + '' + columna2 + '' + columna3 + '' + columna4 + '' + columna5 +
        '' + columna6 + '' + columna7 + '' + columna8 + '' + columna9 + '' + columna10 +
        '' + columna11 + '' + columna12 + '' + columna13 + '' + columna14 + '' + columna15 +
        '' + columna16 + '' + columna17 + '' + columna18 + '' + columna19 + '' + columna20 +
        '' + columna21 + '' + columna22 + '' + columna23 + '' + columna24 + '' + columna25;
      cont_reg++;
    }

    function obtenerDDD(idCity) {
      var ddd = '';

      var city = search.lookupFields({
        type: 'customrecord_lmry_city',
        id: idCity,
        columns: ['custrecord_lmry_city_ddd']
      });

      if (city != null) {
        if (city.custrecord_lmry_city_ddd == '' || city.custrecord_lmry_city_ddd == null) {
          log.debug('Alerta en obtenerDDD', 'La ciudad no tiene un DDD configurado');
        } else {
          ddd = city.custrecord_lmry_city_ddd;
        }
      } else {
        log.debug('Alerta en obtenerDDD', 'No se encuentra la ciudad con id: ' + idCity);
      }

      return ddd;
    }

    function CargarFinal() {
      //Tipo
      var columna1 = 'T9';
      //CNPJ de Contribuyente
      var columna2 = ValidaGuion(CNPJ);
      //MOFG
      var columna3 = anio_date + '' + mes_date;
      //Situación
      var columna4 = cod_cadastro_Situacion; /////********Setup
      //Data do Evento
      if (columna4 == '0') {
        var columna5 = '00000000';
      } else {
        var columna5 = anio_date + '' + mes_date + '' + dia_actual;
      }
      //LINEAS GENERADAS
      var columna6 = '' + (cont_reg + 1);
      columna6 = completar(5, columna6, '0', true);
      //LIMITADOR
      var columna7 = '\r\n';
      strArchivo = strArchivo + '' + columna1 + '' + columna2 + '' + columna3 + '' + columna4 +
        '' + columna5 + '' + columna6 + '' + columna7;
    }

    function CargarPayRoll() {
      var id_payroll = Var_Acount_Payroll.split(',');
      var intDMinReg = 0;
      var intDMaxReg = 1000;

      var savedsearch = search.create({
        type: 'journalentry',
        filters: [
          ["type", "anyof", "Journal"],
          "AND",
          ["postingperiod", "is", param_Periodo],
          "AND",
          ["posting", "is", "T"],
          "AND",
          ["voided", "is", "F"],
          "AND",
          ["memorized", "is", "F"],
          "AND",
          ["formulatext: {custbody_lmry_type_concept.id}", "is", "1"]
        ],
        settings: [
          search.createSetting({
            name: 'consolidationtype',
            value: 'NONE'
          })
        ],
        columns: [
          //00 DebitAmount - CreditAmount
          search.createColumn({
            name: "formulacurrency",
            summary: "SUM",
            formula: "nvl({debitamount},0) - nvl({creditamount},0)",
            label: "Formula (Currency)"
          }),
          search.createColumn({
            name: "formulatext",
            summary: "GROUP",
            formula: "{custbody_lmry_type_concept.name}",
            label: "Formula (text)"
          })
        ]
      });
      var Account_Filter = search.createFilter({
        name: 'account',
        operator: search.Operator.ANYOF,
        values: id_payroll
      });
      savedsearch.filters.push(Account_Filter);

      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.ANYOF,
          values: SubsidiariasContempladas
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      if (feature_Multi) {
        var multibook_Filter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch.filters.push(multibook_Filter);
        //02 Exchange Rate / Multibook
        var exchangerate_Column = search.createColumn({
          name: "formulacurrency",
          summary: "SUM",
          formula: "nvl({accountingtransaction.debitamount},0) - nvl({accountingtransaction.creditamount},0)",
          label: "Formula (Currency)"
        });

        savedsearch.columns.push(exchangerate_Column);
      }
      var searchresult = savedsearch.run();
      var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
      var intLength = objResult.length;

      if (intLength > 0) {
        for (var i = 0; i < intLength; i++) {
          var columns = objResult[i].columns;

          if (feature_Multi) {
            var amount_journal = objResult[i].getValue(columns[2]);
          } else {
            var amount_journal = objResult[i].getValue(columns[0]);
          }
          monto_payroll = Number(monto_payroll) + Number(amount_journal);
          log.error('monto_payroll', monto_payroll);

          if (objResult[i].getValue(columns[1]) != '' && objResult[i].getValue(columns[1]) != null) {
            name_type_concepto = objResult[i].getValue(columns[1]);
          } else {
            name_type_concepto = '';
          }
          log.error('name_type_concepto', name_type_concepto);
        }
      }
    }

    function CargarPayRollCPRB() {
      var id_payroll = Var_Acount_Payroll.split(',');
      var intDMinReg = 0;
      var intDMaxReg = 1000;

      var savedsearch = search.create({
        type: 'journalentry',

        filters: [
          ["type", "anyof", "Journal"],
          "AND",
          ["postingperiod", "is", param_Periodo],
          "AND",
          ["posting", "is", "T"],
          "AND",
          ["voided", "is", "F"],
          "AND",
          ["memorized", "is", "F"],
          "AND",
          ["formulatext: {custbody_lmry_type_concept.id}", "is", "4"]
        ],
        settings: [
          search.createSetting({
            name: 'consolidationtype',
            value: 'NONE'
          })
        ],
        columns: [
          //00 DebitAmount - CreditAmount
          search.createColumn({
            name: "formulacurrency",
            summary: "SUM",
            formula: "nvl({debitamount},0) - nvl({creditamount},0)",
            label: "Formula (Currency)"
          })
        ]
      });
      var Account_Filter = search.createFilter({
        name: 'account',
        operator: search.Operator.ANYOF,
        values: id_payroll
      });

      savedsearch.filters.push(Account_Filter);

      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.ANYOF,
          values: SubsidiariasContempladas
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      if (feature_Multi) {
        var multibook_Filter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch.filters.push(multibook_Filter);
        //02 Exchange Rate / Multibook
        var exchangerate_Column = search.createColumn({
          name: "formulacurrency",
          summary: "SUM",
          formula: "nvl({accountingtransaction.debitamount},0) - nvl({accountingtransaction.creditamount},0)",
          label: "Formula (Currency)"
        });
        savedsearch.columns.push(exchangerate_Column);
      }
      var searchresult = savedsearch.run();
      var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
      var intLength = objResult.length;

      if (intLength > 0) {
        for (var i = 0; i < intLength; i++) {
          var columns = objResult[i].columns;

          if (feature_Multi) {
            var amount_journal = objResult[i].getValue(columns[1]);
          } else {
            var amount_journal = objResult[i].getValue(columns[0]);
          }

          monto_payroll_CPRB = Number(monto_payroll_CPRB) + Number(amount_journal);
          log.error('monto_payroll_CPRB', monto_payroll_CPRB);
        }
      }
    }

    function prepararMatriz(arrImportacion) {
      var lineasImportacionR10 = [];
      var cod_tributo;
      var cod_receita;
      var periodicidad;
      var monto;

      /***********************************  CARGAR JOURNAL IOF Y CIDE *******************************************/
      for (var i = 0; i < arrImportacion.length; i++) {
        cod_tributo = arrImportacion[i][0];
        cod_receita = arrImportacion[i][2];
        periodicidad = arrImportacion[i][3];
        monto = Number(arrImportacion[i][4]).toFixed(2);

        var arrTemp = [cod_tributo, cod_receita, periodicidad, monto];
        lineasImportacionR10.push(arrTemp);
      }

      return lineasImportacionR10
    }

    function prepararRetenciones() {
      var retencionesR10 = [];
      var cod_tributo;
      var cod_receita;
      var periodicidad;
      var monto;

      for (var j = 0; j < Arr_WHT_No_Item.length; j++) {
        cod_tributo = Arr_WHT_No_Item[j][0];
        cod_receita = Arr_WHT_No_Item[j][3];
        periodicidad = Arr_WHT_No_Item[j][5];
        monto = (Arr_WHT_No_Item[j][4]).toFixed(2);

        var arrTemp = [cod_tributo, cod_receita, periodicidad, monto];
        retencionesR10.push(arrTemp);
      }

      for (var j = 0; j < Arr_WHT_Item.length; j++) { //solo es una linea (agrupado IRRF)
        cod_tributo = Arr_WHT_Item[j][0];
        cod_receita = Arr_WHT_Item[j][3];
        periodicidad = Arr_WHT_Item[j][5];
        monto = (Arr_WHT_Item[j][4]).toFixed(2);

        var arrTemp = [cod_tributo, cod_receita, periodicidad, monto];
        retencionesR10.push(arrTemp);
      }

      return retencionesR10
    }

    function prepararPayroll() {
      var payrollR10 = [];
      var cod_tributo = id_tributo_payroll; /** setup **/
      var cod_receita = id_receta_payroll; /** setup **/
      var periodicidad = id_periodicidad_payroll /** setup **/
      var monto;

      if (type_concept_journal == '1') {
        /* CARGAR PLANILLA DE TRABAJADORES */
        if (monto_payroll != null && monto_payroll != 0) {
          monto = monto_payroll.toFixed(2);
          var arrTemp = [cod_tributo, cod_receita, periodicidad, monto];
          payrollR10.push(arrTemp);
        }
      } else {
        /* CARGAR  PLANILLA DE TRABAJADORES CPRB */
        if (monto_payroll_CPRB != null && monto_payroll_CPRB != 0) {
          monto = monto_payroll_CPRB.toFixed(2);
          var arrTemp = [cod_tributo, cod_receita, periodicidad, monto];
          payrollR10.push(arrTemp);
        }
      }

      return payrollR10
    }

    function armarR10(arrayLineas, cnpj_subsi) {
      var lineasR10 = '';
      //Tipo
      var columna1 = 'R10';
      //CNPJ de Contribuyente
      var columna2 = ValidaGuion(CNPJ);
      //MOFG
      var columna3 = anio_date + '' + mes_date;
      //Situación
      var columna4 = cod_cadastro_Situacion; /////********Setup
      //Data do Evento
      if (columna4 == '0') {
        var columna5 = '00000000';
      } else {
        var columna5 = anio_date + '' + mes_date + '' + dia_actual;
      }
      //Grupo de Tributo
      var columna6 = ''; ///Busqueda
      //Código da Receita
      var columna7 = ''; ///Busqueda
      //Periodicidade
      var columna8 = ''; ///Busqueda
      //Ano do Período de Apuração
      var columna9 = anio_date;
      //Mês/Bimestre/Trimestre/Quadrimestre/Semestre do Período de Apuração
      var columna10 = mes_date;
      //Dia/Semana/Quinzena/Decêndio do Período de Apuração
      var columna11 = '00';
      //Ordem do Estabelecimento
      var columna12 = '000000';
      //CNPJ da Incorporação/Matrícula CEI
      var columna13 = '00000000000000';
      //Reservado
      var columna14 = '0';
      //Valor do Débito
      var columna15 = ''; ////Busuqeda
      //Balanço de Redução
      var columna16 = '0';
      //O saldo deste débito será dividido em quotas
      var columna17 = '0';
      //Reservado
      var columna18 = '0';
      //Débito de SCP/INC
      var columna19 = pj_debitos_scp; /** setup **/
      if (columna19 == false || columna19 == 'F') {
        columna19 = '0';
      } else {
        columna19 = '1';
      }
      //Reservado
      var columna20 = '';
      columna20 = completar(10, columna20, ' ', false);
      //Delimitador
      var columna21 = '\r\n';

      for (var i = 0; i < arrayLineas.length; i++) {

        if (arrayLineas[i][0] == '03' || arrayLineas[i][0] == '09') { //IPI o CIDE
          var cnpj_filial = ValidaGuion(cnpj_subsi);
          var long_cnpj_filial = cnpj_filial.length;
          var orden_establecimiento = cnpj_filial.substr(long_cnpj_filial - 6, long_cnpj_filial - 1);
          columna12 = orden_establecimiento;
        }

        if (arrayLineas[i][0] == '01' || arrayLineas[i][0] == '05') { //IRPJ O CSLL
          if (balance_suspensao == 'F' || balance_suspensao == false) {
            columna16 = '1'; ////*****Setup
          } else {
            columna16 = '0'; ////*****Setup
          }
        }
        //COD DEL TRIBUTO
        columna6 = arrayLineas[i][0];
        //COD DE LA RECETA
        columna7 = ValidaGuion(arrayLineas[i][1]);
        //PERIODICIDAD
        columna8 = arrayLineas[i][2];
        //MONTO
        columna15 = completar(14, ValidaGuion(arrayLineas[i][3]), '0', true);

        if (columna15 != '00000000000000') {
          lineasR10 += '' + columna1 + '' + columna2 + '' + columna3 + '' + columna4 + '' + columna5 +
            '' + columna6 + '' + columna7 + '' + columna8 + '' + columna9 + '' + columna10 +
            '' + columna11 + '' + columna12 + '' + columna13 + '' + columna14 + '' + columna15 +
            '' + columna16 + '' + columna17 + '' + columna18 + '' + columna19 + '' + columna20 +
            '' + columna21;
          cont_reg++;
          lineasR10 += ObtenerSubRegistro(columna6, columna7, columna8, columna12, ArrJournalR11);
          lineasR10 += ObtenerSubRegistro(columna6, columna7, columna8, columna12, LineasR12);
          lineasR10 += ObtenerSubRegistro(columna6, columna7, columna8, columna12, LineasR14);
        }

        columna16 = '0';
        columna12 = '000000';
      }

      return lineasR10
    }

    function calcularRealEstimativo(arrayVentasInv, arrayComprasInv, arrayVentas, arrayCompras) {
      var impuestosR10 = [];

      var cod_tributo;
      var cod_receita;
      var periodicidad;
      var impuesto;

      /***********************************  CARGAR SALES (SERVICIOS) *******************************************/
      for (var i = 0; i < arrayVentas.length; i++) {
        cod_tributo = arrayVentas[i][0];
        cod_receita = arrayVentas[i][2];
        periodicidad = arrayVentas[i][3];

        if (arrayVentas[i][0] == '07' || arrayVentas[i][0] == '06') { //PIS Y COFINS *************
          if (arrayCompras.length > 0) {
            for (var j = 0; j < arrayCompras.length; j++) {
              if (arrayVentas[i][0] == arrayCompras[j][0] && arrayVentas[i][2] == arrayCompras[j][2]) {
                impuesto = Number(Number(arrayVentas[i][4]).toFixed(2) - Number(arrayCompras[j][4]).toFixed(2)).toFixed(2);
                arrayCompras.splice(j, 1);
                break;
              } else {
                impuesto = Number(arrayVentas[i][4]).toFixed(2);
              }
            }
          } else {
            impuesto = (Number(arrayVentas[i][4])).toFixed(2);
          }
          var arrTemp = [cod_tributo, cod_receita, periodicidad, impuesto];
          impuestosR10.push(arrTemp);
        }
      }

      for (var i = 0; i < arrayCompras.length; i++) {
        cod_tributo = arrayCompras[i][0];
        cod_receita = arrayCompras[i][2];
        periodicidad = arrayCompras[i][3];

        if (arrayCompras[i][0] == '07' || arrayCompras[i][0] == '06') {
          impuesto = -Number(arrayCompras[i][4]).toFixed(2);
          log.debug("compras sin ventas", impuesto);
          var arrTemp = [cod_tributo, cod_receita, periodicidad, impuesto];
          impuestosR10.push(arrTemp);
        }
      }

      /*********************************  INVENTARIOS (PIS - COFINS) *****************************************/
      for (var i = 0; i < arrayVentasInv.length; i++) {
        cod_tributo = arrayVentasInv[i][0];
        cod_receita = arrayVentasInv[i][2];
        periodicidad = arrayVentasInv[i][3];

        if (arrayVentasInv[i][0] == '03' || arrayVentasInv[i][0] == '06' || arrayVentasInv[i][0] == '07') {
          if (arrayComprasInv.length > 0) {
            for (var j = 0; j < arrayComprasInv.length; j++) {
              if (arrayVentasInv[i][0] == arrayComprasInv[j][0] && arrayVentasInv[i][2] == arrayComprasInv[j][2]) {
                impuesto = Number(Number(arrayVentasInv[i][4]).toFixed(2) - Number(arrayComprasInv[j][4]).toFixed(2)).toFixed(2);
                arrayComprasInv.splice(j, 1);
                break;
              } else {
                impuesto = Number(arrayVentasInv[i][4]).toFixed(2);
              }
            }
          } else {
            impuesto = (Number(arrayVentasInv[i][4])).toFixed(2);
          }

          var arrTemp = [cod_tributo, cod_receita, periodicidad, impuesto];
          impuestosR10.push(arrTemp);
        }

      }

      for (var i = 0; i < arrayComprasInv.length; i++) {
        cod_tributo = arrayComprasInv[i][0];
        cod_receita = arrayComprasInv[i][2];
        periodicidad = arrayComprasInv[i][3];

        if (arrayComprasInv[i][0] == '03' || arrayComprasInv[i][0] == '07' || arrayComprasInv[i][0] == '06') {
          impuesto = -Number(arrayComprasInv[i][4]).toFixed(2);
          log.debug("compras sin ventas inventario", impuesto);
          var arrTemp = [cod_tributo, cod_receita, periodicidad, impuesto];
          impuestosR10.push(arrTemp);
        }
      }

      /************************************  CARGAR IRPJ ********************************************/
      if (param_Lucro_Conta != 0) {
        cod_tributo = '01';
        cod_receita = id_receta_irpj; /*** setup **/
        periodicidad = id_periodicidad_irpj; /** setup **/

        var calculoIRPJ = Number(param_Lucro_Conta) + Number(param_Monto_Adicional) - Number(param_Monto_Excluyente);
        porc_alicuota_irpj = Number(ValidaPorcentaje(porc_alicuota_irpj)) / 100; /** setup **/
        porc_adicional_irpj = Number(ValidaPorcentaje(porc_adicional_irpj)) / 100; /** setup **/

        if (calculoIRPJ < Number(monto_limite)) {
          impuesto = (calculoIRPJ * porc_alicuota_irpj).toFixed(2);
        } else {
          impuesto = (calculoIRPJ * porc_alicuota_irpj + (calculoIRPJ - (Number(monto_limite) * Number(mes_date))) * porc_adicional_irpj).toFixed(2);
        }

        var arrTemp = [cod_tributo, cod_receita, periodicidad, impuesto];
        impuestosR10.push(arrTemp);
      }

      /************************************ CARGAR CSLL *********************************************/
      if (param_Lucro_Conta != 0) {
        cod_tributo = '05';
        cod_receita = id_receta_csll; /*** setup  **/
        periodicidad = id_periodicidad_csll; /** setup **/

        var calculoCSLL = Number(param_Lucro_Conta) + Number(param_Monto_Adicional) - Number(param_Monto_Excluyente);
        porc_alicuota_csll = Number(ValidaPorcentaje(porc_alicuota_csll)) / 100; /** setup **/
        impuesto = (calculoCSLL * porc_alicuota_csll).toFixed(2);

        var arrTemp = [cod_tributo, cod_receita, periodicidad, impuesto];
        impuestosR10.push(arrTemp);
      }

      log.debug('r10 calculo real estimativo', impuestosR10);
      return impuestosR10;
    }

    function eliminarMontosCeros(arrayImpuestos) {
      var long = arrayImpuestos.length;
      var i = 0;
      while (i < long) {
        if (arrayImpuestos[i][3] == 0.00) {
          arrayImpuestos.splice(i, 1);
          long--;
        } else {
          i++;
        }
      }
      return arrayImpuestos;
    }

    function eliminarMinimos(arrayImpuestos) {
      var long = arrayImpuestos.length;
      var i = 0;
      while (i < long) {
        if (arrayImpuestos[i][3] < Monto_min_imp) {
          arrayImpuestos.splice(i, 1);
          long--;
        } else {
          i++;
        }
      }
      return arrayImpuestos;
    }

    function calcularPresumido(arrayVentasInv, arrayComprasInv, arrayVentas, arrayCompras) {
      var impuestosR10 = [];

      var cod_tributo;
      var cod_receita;
      var periodicidad;
      var impuesto;

      /***********************************  CARGAR SALES  (SERVICIOS) *******************************************/
      for (var i = 0; i < arrayVentas.length; i++) {
        cod_tributo = arrayVentas[i][0];
        cod_receita = arrayVentas[i][2];
        periodicidad = arrayVentas[i][3];
        impuesto = Number(arrayVentas[i][4]).toFixed(2);

        if (arrayVentas[i][0] == '01' || arrayVentas[i][0] == '05') {
          impuesto = Number(Number(percentage_receta) * Number(impuesto)).toFixed(2);
        }

        var arrTemp = [cod_tributo, cod_receita, periodicidad, impuesto];
        impuestosR10.push(arrTemp);
      }

      for (var i = 0; i < arrayVentasInv.length; i++) { // INVENTARIOS ********************
        cod_tributo = arrayVentasInv[i][0];
        cod_receita = arrayVentasInv[i][2];
        periodicidad = arrayVentasInv[i][3];

        if (arrayVentasInv[i][0] == '03') {

          if (arrayComprasInv.length > 0) {
            for (var j = 0; j < arrayComprasInv.length; j++) {
              if (arrayVentasInv[i][0] == arrayComprasInv[j][0] && arrayVentasInv[i][2] == arrayComprasInv[j][2]) {
                impuesto = Number(Number(arrayVentasInv[i][4]).toFixed(2) - Number(arrayComprasInv[j][4]).toFixed(2)).toFixed(2);
                break;
              } else {
                impuesto = Number(arrayVentasInv[i][4]).toFixed(2);
              }
            }
          } else {
            impuesto = Number(arrayVentasInv[i][4]).toFixed(2);
          }

        } else {
          impuesto = (Number(arrayVentasInv[i][4]));

          if (arrayVentasInv[i][0] == '01' || arrayVentasInv[i][0] == '05') {
            impuesto = (Number(percentage_receta) * Number(impuesto));
          }
          impuesto = Number(impuesto).toFixed(2);
        }

        var arrTemp = [cod_tributo, cod_receita, periodicidad, impuesto];
        impuestosR10.push(arrTemp);
      }

      log.debug('r10 calculo presumido', impuestosR10);
      return impuestosR10;
    }

    function ValidaGuion(s) {
      if (s != null && s != '') {
        var AccChars = "+./-() ";
        var RegChars = "";

        s = s.toString();
        for (var c = 0; c < s.length; c++) {
          for (var special = 0; special < AccChars.length; special++) {
            if (s.charAt(c) == AccChars.charAt(special)) {
              s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
            }
          }
        }
      }
      return s;
    }

    function validarCaracteres_Especiales(s) {

      if (s != null && s != '') {
        var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ°&°–—ªº·";
        var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyo  --a .";
        s = s.toString();
        for (var c = 0; c < s.length; c++) {
          for (var special = 0; special < AccChars.length; special++) {
            if (s.charAt(c) == AccChars.charAt(special)) {
              s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
            }
          }
        }

        return s;
      }

      return '';
    }

    function ValidaPorcentaje(s) {
      if (s != null && s != '') {
        var AccChars = "%";
        var RegChars = "";

        s = s.toString();
        for (var c = 0; c < s.length; c++) {
          for (var special = 0; special < AccChars.length; special++) {
            if (s.charAt(c) == AccChars.charAt(special)) {
              s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
            }
          }
        }
      }
      return s;
    }

    function ObtenerDatosSubsidiaria() {
      var configpage = config.load({
        type: config.Type.COMPANY_INFORMATION
      });

      if (feature_Subsi) {
        companyname = ObtainNameSubsidiaria(param_Subsi);
        companyname = validarCaracteres_Especiales(companyname);
        companyruc = ObtainFederalIdSubsidiaria(param_Subsi);
      } else {
        companyruc = configpage.getValue('employerid');
        companyname = configpage.getValue('legalname');
      }

      companyruc = companyruc.replace(' ', '');
      companyruc = ValidaGuion(companyruc);
    }

    function ObtainNameSubsidiaria(subsidiary) {
      try {
        if (subsidiary != '' && subsidiary != null) {
          var subsidyName = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: subsidiary,
            columns: ['legalname']
          });
          return subsidyName.legalname
        }
      } catch (err) {
        //libreria.sendMail(LMRY_script, ' [ ObtainNameSubsidiaria ] ' + err);
      }
      return '';
    }

    function ObtainFederalIdSubsidiaria(subsidiary) {
      try {
        if (subsidiary != '' && subsidiary != null) {
          var federalId = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: subsidiary,
            columns: ['taxidnum']
          });

          return federalId.taxidnum
        }
      } catch (err) {
        //libreria.sendMail(LMRY_script, ' [ ObtainFederalIdSubsidiaria ] ' + err);
      }
      return '';
    }

    function completar(long, valor, caracter, alinear) {
      if (valor == null) {
        valor = '';
      }

      if ((('' + valor).length) <= long) {
        if (long != ('' + valor).length) {
          for (var i = ('' + valor.length); i < long; i++) {
            if (alinear) {
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
        valor = ('' + valor).substring(0, long);
        return valor;
      }
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

    function ObtenerParametrosYFeatures() {
      //PARAmetros
      param_RecorID = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_recorid'
      });
      param_Periodo = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_periodo'
      });
      param_Subsi = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_subsi'
      });
      param_Multi = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_multi'
      });
      param_Type_Decla = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_decla'
      });
      param_Num_Recti = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_recti'
      });
      param_archivo = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_archivo'
      });
      param_Lucro_Conta = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_lucro_conta'
      });
      param_Monto_Adicional = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_monto_adic'
      });
      param_Monto_Excluyente = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_monto_excluy'
      });
      param_Feature = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_dec_featureid'
      });

      log.debug({
        title: 'Parametros',
        details: param_RecorID + '-' + param_Periodo + '-' + param_Subsi + '-' + param_Multi + '-' + param_Type_Decla + '-' + param_Num_Recti + '-' + param_Lucro_Conta + '-' + param_Feature + '-' + param_archivo
      });

      if (param_Lucro_Conta == null) {
        param_Lucro_Conta = 0;
      }
      if (param_Monto_Adicional == null) {
        param_Monto_Adicional = 0;
      }
      if (param_Monto_Excluyente == null) {
        param_Monto_Excluyente = 0;
      }
      //************FEATURES********************
      feature_Subsi = runtime.isFeatureInEffect({
        feature: "SUBSIDIARIES"
      });
      feature_Multi = runtime.isFeatureInEffect({
        feature: "MULTIBOOK"
      });
      var licenses = libFeature.getLicenses(param_Subsi);
      featAccountingSpecial = libFeature.getAuthorization(599, licenses);
      log.debug('featAccountingSpecial', featAccountingSpecial);
      //**************PERIODO********************
      var periodenddate_temp = null;

      if (featAccountingSpecial) {

        var specialAccountingSearch = search.create({
          type: "customrecord_lmry_special_accountperiod",
          filters: [
            ['isinactive', 'is', 'F'], "AND",
            ["custrecord_lmry_accounting_period", "anyof", param_Periodo]
          ],
          columns: [
            search.createColumn({
              name: "name",
              label: "Name"
            }),
            search.createColumn({
              name: "custrecord_lmry_date_ini",
              sort: search.Sort.ASC,
              label: "Latam - Date Start"
            }),
            search.createColumn({
              name: "custrecord_lmry_date_fin",
              label: "Latam - Date Finish"
            })
          ]
        });

        var result = specialAccountingSearch.run().getRange(0, 1);
        log.debug('result special accounting', result);
        if (result.length != 0) {
          for (var i = 0; i < result.length; i++) {
            periodenddate_temp = {
              periodname: result[i].getValue('name'),
              enddate: result[i].getValue('custrecord_lmry_date_fin'),
              startdate: result[i].getValue('custrecord_lmry_date_ini')
            };
          }
        } else {
          log.debug('Alerta', 'No se configuró periodo en Special Accounting Period');
          periodenddate_temp = search.lookupFields({
            type: search.Type.ACCOUNTING_PERIOD,
            id: param_Periodo,
            columns: ['enddate', 'periodname', 'startdate']
          });
        }

      } else {
        periodenddate_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_PERIOD,
          id: param_Periodo,
          columns: ['enddate', 'periodname', 'startdate']
        });
      }
      log.debug('periodenddate_temp', periodenddate_temp);
      if (periodenddate_temp != null) {
        //Period EndDate
        periodenddate = periodenddate_temp.enddate;
        //Nuevo Formato Fecha
        var parsedDateStringAsRawDateObject = format.parse({
          value: periodenddate,
          type: format.Type.DATE
        });

        mes_date = parsedDateStringAsRawDateObject.getMonth() + 1;
        anio_date = parsedDateStringAsRawDateObject.getFullYear();
        dia_date = parsedDateStringAsRawDateObject.getDate();

        PeriodAnt = obtenerPeriodoAnt(anio_date, mes_date);
        mes_date += '';
        anio_date += '';

        if (dia_date.length == 1) {
          dia_date = '0' + dia_date;
        }
        if ((mes_date + '').length == 1) {
          mes_date = '0' + mes_date;
        }
        periodenddate = dia_date + mes_date + anio_date;
        //Period Startdate
        var periodfirstdate = periodenddate_temp.startdate;
        //Nuevo Formato Fecha
        var parsedDateStringAsRawDateObject = format.parse({
          value: periodfirstdate,
          type: format.Type.DATE
        });

        mes_ini = parsedDateStringAsRawDateObject.getMonth() + 1;
        mes_ini += '';
        anio_ini = parsedDateStringAsRawDateObject.getFullYear() + '';
        dia_ini = parsedDateStringAsRawDateObject.getDate() + '';

        if (dia_date.length == 1) {
          dia_ini = '0' + dia_ini;
        }
        if ((mes_ini + '').length == 1) {
          mes_ini = '0' + mes_ini;
        }
        //Period Name
        periodname = periodenddate_temp.periodname;
      } else {
        log.debug('Alerta Periodo', 'No se tiene configurado el periodo contable');
      }

      //FECHA ACTUAL
      var f = new Date();
      dia_actual = String(f.getDate());
      mes_actual = f.getMonth() + 1;
      mes_actual = String(mes_actual);

      if (mes_actual.length == 1) {
        mes_actual = '0' + mes_actual;
      }
      if (dia_actual.length == 1) {
        dia_actual = '0' + dia_actual;
      }
      /******************************** DATOS DE SUBSIDIARIA ********************************/
      var id_subsi = 0;
      if (feature_Subsi) {
        id_subsi = param_Subsi;
      } else {
        id_subsi = 1;
      }

      var subsi_temp = search.lookupFields({
        type: search.Type.SUBSIDIARY,
        id: id_subsi,
        columns: ['taxidnum', 'custrecord_lmry_br_uf', 'address.zip', 'custrecord_lmry_br_cef', 'address.address1', 'address.phone', 'custrecord_lmry_br_porc_economic_activiy', 'custrecord_lmry_br_regimen_pis_confis', 'address.custrecord_lmry_addr_prov_acronym', 'address.custrecord_lmry_address_number', 'address.custrecord_lmry_addr_reference', 'address.address2', 'address.custrecord_lmry_addr_city', 'custrecord_lmry_email_subsidiaria']
      });

      CNPJ = subsi_temp.taxidnum; //cnpj='00.520.964/0001-60';
      barrio = subsi_temp['address.address2'];

      var cityArrayJson = subsi_temp['address.custrecord_lmry_addr_city'];
      if (cityArrayJson != null && cityArrayJson.length != 0) {
        ddd_subsi = obtenerDDD(cityArrayJson[0].value);
        city = cityArrayJson[0].text;
      } else {
        log.debug('Alerta en ObtenerParametrosYFeatures', 'No se ha configurado Ciudad para la Subsidiaria');
        ddd_subsi = '';
        city = '';
      }

      complemento = subsi_temp['address.custrecord_lmry_addr_reference'];
      num_dir = subsi_temp['address.custrecord_lmry_address_number'];

      correo = subsi_temp.custrecord_lmry_email_subsidiaria;
      uf_domicilio = subsi_temp.custrecord_lmry_br_uf;

      if (uf_domicilio != null && uf_domicilio != '') {
        uf_domicilio = uf_domicilio[0].value;
        var uf_domicilio_temp = search.lookupFields({
          type: 'customrecord_lmry_br_uf',
          id: uf_domicilio,
          columns: ['custrecord_lmry_br_code_uf']
        });

        uf_domicilio = uf_domicilio_temp.custrecord_lmry_br_code_uf;
      } else {
        uf_domicilio = '';
      }

      zip = subsi_temp['address.zip'];
      uf_subsi = subsi_temp['address.custrecord_lmry_addr_prov_acronym'];
      cef = subsi_temp.custrecord_lmry_br_cef;
      address = subsi_temp['address.address1'];
      phone = subsi_temp['address.phone'];
      percentage_receta = subsi_temp.custrecord_lmry_br_porc_economic_activiy;
      reg_pis_confis = subsi_temp.custrecord_lmry_br_regimen_pis_confis;

      if (reg_pis_confis != null && reg_pis_confis != '') {
        reg_pis_confis = reg_pis_confis[0].value;
        var reg_pis_confis_temp = search.lookupFields({
          type: 'customrecord_lmry_br_determinat_regime',
          id: reg_pis_confis,
          columns: ['custrecord_lmry_br_code_det_regime']
        });

        reg_pis_confis = reg_pis_confis_temp.custrecord_lmry_br_code_det_regime;
      } else {
        reg_pis_confis = '';
      }
      /**************** DATOS MULTIBOOK ****************/
      if (feature_Multi) {
        //Multibook Name
        var multibookName_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_BOOK,
          id: param_Multi,
          columns: ['name']
        });
        multibookName = multibookName_temp.name;
      }
      //percentage_receta
      percentage_receta = ValidaPorcentaje(percentage_receta);
      percentage_receta = Number(percentage_receta) / 100;
      /**************** DATOS FEATURE ****************/
      var featureReport = search.lookupFields({
        type: 'customrecord_lmry_br_features',
        id: param_Feature,
        columns: ['custrecord_lmry_br_version']
      });

      version = featureReport.custrecord_lmry_br_version;
    }

    return {
      execute: execute
    };
  });
