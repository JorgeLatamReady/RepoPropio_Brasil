/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for Inventory Balance Library                  ||
||                                                              ||
||  File Name: LMRY_BR_DCTF_XLS_SCHDL_V2.0.js                   ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     May 15 2020  LatamReady    Use Script 2.0           ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.x
 * @NScriptType ScheduledScript
 * @NModuleScope Public
 */
define(["N/record", "N/runtime", "N/file", "N/email", "N/search", "N/format",
    "N/log", "N/config", "N/task", 'N/encode', "./BR_LIBRERIA_MENSUAL/LMRY_BR_Reportes_LBRY_V2.0.js",
    "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_libSendingEmailsLBRY_V2.0.js"
  ],

  function(recordModulo, runtime, fileModulo, email, search, format, log,
    config, task, encode, libreria, libFeature) {

    var objContext = runtime.getCurrentScript();
    // Nombre del Reporte
    var LMRY_script = 'LMRY_BR_DCTF_XLS_SCHDL_V2.0.js';
    //Parametros
    var param_RecordLogID = null;
    var param_Periodo = null;
    var param_Subsi = null;
    var param_Type_Decla = null;
    var param_Num_Recti = null;
    var param_Multi = null;
    var param_Feature = null
    var param_Lucro_Conta = null;
    var param_Monto_Adicional = null;
    var param_Monto_Excluyente = null;
    var param_File = null;
    //var file_size = 7340032;
    var language = runtime.getCurrentScript().getParameter({
      name: 'LANGUAGE'
    }).substring(0, 2);

    //Features
    var feature_Subsi = null;
    var feature_Multi = null;
    var featAccountingSpecial = null;
    //Datos de Subsidiaria
    var companyname = null;
    var companyruc = null;
    //Retenciones
    var strRetenciones = '';
    var strRetenciones_aux = '';
    //Period enddate
    var mes_date = null;
    var anio_date = null;
    var periodname = null;
    //Datos de subsidiaria
    var percentage_receta = null;
    //Nombre de libro contable
    var multibookName = '';
    var NameReport = '';
    //Valores del SetupDCTF ********************
    var cod_form_tribut = null;
    var reg_pis_confis = null; //
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
    var id_item_repres_legal = null;
    //-.-.-.-.-.-Arreglos para Inventariado-.-.-.-.-.-.-.-.-.-
    var ArrSetupDCTF_Sales_Inv = new Array();
    var ArrSetupDCTF_Purchases_Inv = new Array();
    //-.-.-.-.-.-.-.-.-.--.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.--.
    var ArrPayroll = new Array();
    var ArrPayrollCPRB = new Array();
    var ArrSetupDCTF_Sales = new Array();
    var ArrSetupDCTF_Purchases = new Array();
    var Arr_WHT_Item = new Array();
    var Arr_WHT_No_Item = new Array();
    var ArrIOFBillPay = new Array();
    var ArrCIDEBillPay = new Array();
    var ArrJournalImportacion = new Array();
    var ArrJournalImportacionTemp = new Array();
    var ArrJournalR12 = new Array();
    var LineasR12 = new Array();
    var ArrJournalR14 = new Array();
    var LineasR14 = new Array();
    var VectorPagosTributos = new Array();
    var JournalsCoinciR11 = new Array();
    var Var_Acount_Payroll = null;
    var Type_concept_journal = null;
    var Filiales;
    var SubsidiariasContempladas;
    var ArrLineasNomina = new Array();
    var ArrEPayLines = new Array();
    var ArrEPayLinesPago = new Array();
    var ArrEPayLinesNomina = new Array();

    function execute(context) {
      try {
        ObtenerParametrosYFeatures();
        ObtnerSetupRptDCTF();
        ObtenerDatosSubsidiaria();

        if (Filiales != '' && Filiales != null) {
          Filiales = Filiales.split(',');
          log.debug('Filiales', Filiales);
          SubsidiariasContempladas = Filiales;
          SubsidiariasContempladas.push(param_Subsi);
        } else {
          SubsidiariasContempladas = [param_Subsi];
        }

        ObtenerWHT();
        log.debug('array retencion No Item', Arr_WHT_No_Item);
        log.debug('array retencion Item', Arr_WHT_Item);

        GenerarArreglos();
        ArrSetupDCTF_Sales = agruparLineas(ArrSetupDCTF_Sales);
        ArrSetupDCTF_Purchases = agruparLineas(ArrSetupDCTF_Purchases);
        ArrSetupDCTF_Sales_Inv = agruparLineas(ArrSetupDCTF_Sales_Inv);
        ArrSetupDCTF_Purchases_Inv = agruparLineas(ArrSetupDCTF_Purchases_Inv);
        ArrJournalImportacion = agruparLineas(ArrJournalImportacion); //importaciones proceso manual
        ArrIOFBillPay = agruparLineas(ArrIOFBillPay); //importaciones proceso automatico
        ArrCIDEBillPay = agruparLineas(ArrCIDEBillPay); //importaciones proceso automatico

        if (id_account_payroll != '' && id_account_payroll != '- None -' && id_account_payroll != null) {
          log.debug('cuentas payroll', Var_Acount_Payroll);
          CargarPayRoll();
          CargarPayRollCPRB();
        }

        arrRecetasVentas = ObtenerRecetasVentas();
        //R11
        VectorPagosTributos = ObtenerDataPagosTributos(5); //Pagos de Impuesto
        ArrLineasNomina = ObtenerDataPagosTributos(25); //Pagamento Impostos de Nomina
        VectorPagosTributos = agruparR11(VectorPagosTributos);
        ArrLineasNomina = agruparR11(ArrLineasNomina);
        formatEPayLines(ArrEPayLines); //formateara la data a la estructura de datos que se tiene para pagos por Journal
        VectorPagosTributos = VectorPagosTributos.concat(ArrEPayLinesPago);
        ArrLineasNomina = ArrLineasNomina.concat(ArrEPayLinesNomina);
        //R12
        ArrJournalR12 = obtenerJournalsR12_R14('R12');
        //R14
        ArrJournalR14 = obtenerJournalsR12_R14('R14');
        ArrJournalR14 = agruparR14(ArrJournalR14);

        CargarCabeceraEstilosExcel();
        if (cod_form_tribut == '1') {
          CargaDatosDebCredRealEstimativo();
        } else {
          CargaDatosDebCredPresumido();
        }

        if (ArrSetupDCTF_Sales.length == 0 && ArrSetupDCTF_Sales_Inv.length == 0 && ArrSetupDCTF_Purchases.length == 0 && ArrSetupDCTF_Purchases_Inv.length == 0 && ArrPayroll.length == 0 && Arr_WHT_Item.length == 0 && Arr_WHT_No_Item.length == 0 && ArrJournalImportacion.length == 0 && param_Monto_Excluyente == 0 && param_Monto_Adicional == 0 && param_Lucro_Conta == 0) {
          NoData(false);
        } else {
          SaveFile();
        }
      } catch (error) {
        libreria.sendemailTranslate(error, LMRY_script, language);
        NoData(true);
      }
    }

    function GenerarArreglos() {
      param_File = Number(param_File);
      var arrAuxiliar;
      var string = fileModulo.load({
        id: param_File
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
        if (arrAuxiliar[0] == 'CustInvc') {
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
        if (arrAuxiliar[0] == 'VendBill') {
          if (arrAuxiliar[7] == '99') {
            ArrSetupDCTF_Purchases.push(arrAuxiliar);
          } else if (arrAuxiliar[7] == '55') {
            ArrSetupDCTF_Purchases_Inv.push(arrAuxiliar);
          } else if (arrAuxiliar[7] == 'DF') {
            ArrEPayLines.push(arrAuxiliar); //los que tienen multas y juros
          }
        }
      }

      for (var j = 0; j < arrAuxiliarJournal.length; j++) {
        if (arrAuxiliarJournal[j] == '') {
          break;
        }
        arrAuxiliar = arrAuxiliarJournal[j].split(';');
        if (arrAuxiliar[0] == 'Journal') {
          if (arrAuxiliar[11] == 'm') {
            ArrJournalImportacion.push(arrAuxiliar);
          }
          ArrJournalImportacionTemp.push(arrAuxiliar);
        }
      }

      for (var j = 0; j < arrAuxiliarBillPayment.length; j++) {
        if (arrAuxiliarBillPayment[j] == '') {
          break;
        }
        arrAuxiliar = arrAuxiliarBillPayment[j].split(';');
        if (arrAuxiliar[0] == 'VendPymt') {
          if (arrAuxiliar[7] == 'DF') {
            ArrEPayLines.push(arrAuxiliar);
          } else if (arrAuxiliar[1] == '04') {
            ArrIOFBillPay.push(arrAuxiliar);
          } else if (arrAuxiliar[1] == '09') {
            ArrCIDEBillPay.push(arrAuxiliar);
          }
        }
      }

      log.debug('valor de arreglo de Ventas- Servicios', ArrSetupDCTF_Sales);
      log.debug('valor de arreglo de Ventas - INventario', ArrSetupDCTF_Sales_Inv);
      log.debug('valor de arreglo de compras - Servicios', ArrSetupDCTF_Purchases);
      log.debug('vaor de arreglo de comrpas - Inventario', ArrSetupDCTF_Purchases_Inv);
      log.debug('Journals de Importacion', ArrJournalImportacion);
      log.error('vaor de arreglo de Bill Pay IOF', ArrIOFBillPay);
      log.error('vaor de arreglo de Bill Pay CIDE', ArrCIDEBillPay);
      log.error('vaor de arreglo de Bill Pay E Payment', ArrEPayLines);
    }

    function ObtenerRecetasVentas() {
      var arrAuxiliar = new Array();

      for (var i = 0; i < ArrSetupDCTF_Sales.length; i++) {
        arrAuxiliar.push(ArrSetupDCTF_Sales[i][3]);
      }
      for (var j = 0; j < ArrSetupDCTF_Sales_Inv.length; j++) {
        arrAuxiliar.push(ArrSetupDCTF_Sales_Inv[j][3]);
      }

      arrAuxiliar = arrAuxiliar.filter(function(value, index, self) {
        return self.indexOf(value) === index;
      });
      log.debug('valores ya no repetidos', arrAuxiliar);
      return arrAuxiliar;
    }

    function ObtenerWHT() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var comilla = "'";

      var DbolStop = false;
      var arrAuxiliar = new Array();

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

      //10
      var columna_internalid = search.createColumn({
        name: "internalid",
        label: "Internal ID"
      });
      savedsearch.columns.push(columna_internalid);
      //11
      var columna_numdoc = search.createColumn({
        name: "tranid",
        label: "Document Number"
      });
      savedsearch.columns.push(columna_numdoc);

      if (feature_Multi) {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch.filters.push(multibookFilter);
        //12
        var exchange_rate_multi = search.createColumn({
          name: "exchangerate",
          join: "accountingtransaction"
        });
        savedsearch.columns.push(exchange_rate_multi);
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
            //1. Id Tributo
            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            //2. Tributo
            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            //3. Receta
            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
            //4. Id Receta
            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            //5. Monto
            arrAuxiliar[4] = redondear(Number(objResult[i].getValue(columns[4])));
            arrAuxiliar[4] = arrAuxiliar[4] * exch_rate_nf;
            arrAuxiliar[4] = arrAuxiliar[4].toFixed(2);

            var whtLocalCurrency = objResult[i].getValue(columns[9]);
            //log.debug('whtLocalCurrency',whtLocalCurrency);
            if (whtLocalCurrency != null && whtLocalCurrency != 0 && whtLocalCurrency != '- None -') {
              arrAuxiliar[4] = redondear(whtLocalCurrency);
            } else {
              //log.debug('no tiene local currency', arrAuxiliar);
              //Exchange Rate
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                var exch_rate_nf = objResult[i].getValue(columns[5]);
                exch_rate_nf = exchange_rate(exch_rate_nf);
              } else {
                if (feature_Multi) {
                  var exch_rate_nf = objResult[i].getValue(columns[12]);
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
            //7. Doc. Number de la transaction
            arrAuxiliar[6] = objResult[i].getValue(columns[11]);

            if (objResult[i].getValue(columns[7]) == id_item_repres_legal) {
              Arr_WHT_Item.push(arrAuxiliar);
            } else {
              Arr_WHT_No_Item.push(arrAuxiliar);
            }

          }
          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
        } else {
          DbolStop = true;
        }
      }
    }

    function redondear(number) {
      return Math.round(Number(number) * 100) / 100;
    }

    function GenerarHojaPagosJournal(VectorPagosTributosParam) {
      var nameHojaJournal = 'Impuestos Pagados';
      var VectorPagosTributosOrden = [];
      var strSheetJournals = '';
      //Aqui debe ponerse el vector con journals q si tienen deuda de tributo
      if (VectorPagosTributosParam.length != 0) {
        strSheetJournals += '<Worksheet ss:Name="' + nameHojaJournal + '">';
        strSheetJournals += '<Table>';

        strSheetJournals += CargarTituloHojaExcel(6, 'Datos relacionado a Impuestos Pagados - R11');
        strSheetJournals += '<Row>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> # DOC </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> TIPO DOC </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> FECHA DE PAGO </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> FECHA VENCIMIENTO </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> TRIBUTO </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> COD. TRIBUTO </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> RECEITA </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> PERIODICIDAD</Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR PRINCIPAL</Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR DE MULTA</Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR DE JUROS</Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR DE PAGO</Data></Cell>' +
          '</Row>';

        log.debug('Todos los Journals sin ordenar', VectorPagosTributosParam);
        //Ordenar Vector de Journals por Cod de Tributo
        VectorPagosTributosOrden = OrdenarJournalsTributo(VectorPagosTributosParam);
        log.debug('VectorPagosTributosOrden', VectorPagosTributosOrden);

        for (var i = 0; i < VectorPagosTributosOrden.length; i++) {
          strSheetJournals += '<Row>';
          for (var j = 0; j < VectorPagosTributosOrden[i].length; j++) {
            strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + VectorPagosTributosOrden[i][j] + '</Data></Cell>';
          }
          strSheetJournals += '</Row>';
        }

        strSheetJournals += '</Table>'
        strSheetJournals += '</Worksheet>';
      }
      return strSheetJournals;
    }

    function GenerarHojaCompensacionesJournal(lineasJournal) {
      var nameHojaJournal = 'Compensaciones';
      var strSheetJournals = '';

      //Aqui debe ponerse el vector con journals q si tienen deuda de tributo
      if (lineasJournal.length != 0) {
        strSheetJournals += '<Worksheet ss:Name="' + nameHojaJournal + '">';
        strSheetJournals += '<Table>';

        strSheetJournals += CargarTituloHojaExcel(4, 'Datos relacionado a Compensaciones - R12');
        strSheetJournals += '<Row>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> # DOC </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> TIPO DOC </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> FECHA DE PAGO </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> TRIBUTO </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> COD. TRIBUTO </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> RECEITA </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> PERIODICIDAD</Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR DE COMPENSACION</Data></Cell>' +
          '</Row>';

        log.debug('Todos los Journals sin ordenar compensaciones', lineasJournal);
        for (var i = 0; i < lineasJournal.length; i++) {
          strSheetJournals += '<Row>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][0] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">Journal</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][1] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][2] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][3] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][4] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][5] + '</Data></Cell>';
          //MONTO
          var monto = Number(lineasJournal[i][6]).toFixed(2);
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + monto + '</Data></Cell>';
          strSheetJournals += '</Row>';
        }
        strSheetJournals += '</Table>'
        strSheetJournals += '</Worksheet>';
      }
      return strSheetJournals;
    }

    function GenerarHojaSuspensionesJournal(lineasJournal) {
      var nameHojaJournal = 'Suspensiones';
      var strSheetJournals = '';

      //Aqui debe ponerse el vector con journals q si tienen deuda de tributo
      if (lineasJournal.length != 0) {
        strSheetJournals += '<Worksheet ss:Name="' + nameHojaJournal + '">';
        strSheetJournals += '<Table>';

        strSheetJournals += CargarTituloHojaExcel(5, 'Datos relacionado a Suspensiones - R14');
        strSheetJournals += '<Row>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> # DOC </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> TIPO DOC </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> FECHA DE PAGO </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> TRIBUTO </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> COD. TRIBUTO </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> RECEITA </Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> PERIODICIDAD</Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR DE SUSPENSIÓN</Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR PRINCIPAL</Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR DE MULTA</Data></Cell>' +
          '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR DE JUROS</Data></Cell>' +
          '</Row>';

        log.debug('Todos los Journals sin ordenar suspensiones', lineasJournal);
        for (var i = 0; i < lineasJournal.length; i++) {
          strSheetJournals += '<Row>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][0] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">Journal</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][1] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][2] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][3] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][4] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][5] + '</Data></Cell>';
          //MONTO
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][6] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][7] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][8] + '</Data></Cell>';
          strSheetJournals += '<Cell ss:StyleID="s25" ><Data ss:Type="String">' + lineasJournal[i][9] + '</Data></Cell>';
          strSheetJournals += '</Row>';
        }
        strSheetJournals += '</Table>'
        strSheetJournals += '</Worksheet>';
      }
      return strSheetJournals;
    }

    function OrdenarJournalsTributo(VectorPagosTributos) {
      var temporal = [];
      for (var i = 0; i < VectorPagosTributos.length; i++) {
        for (var j = 0; j < VectorPagosTributos.length - 1; j++) {

          var Codigo1 = Number(VectorPagosTributos[j][5]);
          var Codigo2 = Number(VectorPagosTributos[j + 1][5]);
          //log.debug('codigos', Codigo1 + '|' + Codigo2);
          if (Codigo1 > Codigo2) {
            temporal = VectorPagosTributos[j];
            VectorPagosTributos[j] = VectorPagosTributos[j + 1];
            VectorPagosTributos[j + 1] = temporal;
          }
        }
      }
      return VectorPagosTributos;
    }
    /**
     * Agrupa lineas de acuerdo a tributo y receita
     */
    function agruparLineas(matrizGeneral) {
      var matrizOrdenada = [];
      var long = matrizGeneral.length;
      var i = 0;

      while (i < long) {
        var matrizTemporal = [];
        matrizTemporal.push(matrizGeneral[i]);
        var j = i + 1;
        /* SE AGRUPAN LOS ELEMENTOS EN UNA MATRIZ TEMPORAL */
        while (j < long) {
          if (matrizGeneral[j][2] == matrizGeneral[i][2] && matrizGeneral[j][3] == matrizGeneral[i][3]) {
            matrizTemporal.push(matrizGeneral[j]);
            matrizGeneral.splice(j, 1);
            long--;
          } else {
            j++;
          }
        }
        /* SE AGREGA A LA MATRIZ GENERAL ORDENADA */
        if (matrizOrdenada.length != 0) {
          var posicion = -1;
          for (var m = 0; m < matrizOrdenada.length; m++) {
            if (matrizOrdenada[m][2] == matrizGeneral[i][2]) {
              posicion = m;
              break;
            }
          }

          if (posicion != -1) { //Si no se el ultimo elemento del recorrido
            /* llenado de matriz actualizada */
            for (var k = 0; k < matrizTemporal.length; k++) {
              matrizOrdenada.splice(posicion, 0, matrizTemporal[k]);
            }
          } else { //si es el ultimo elemento
            var matrizUnida = matrizOrdenada.concat(matrizTemporal);
            matrizOrdenada = matrizUnida;
          }

        } else {
          matrizOrdenada = matrizTemporal;
        }

        i++;
      }
      log.debug('matrizOrdenada', matrizOrdenada);
      return matrizOrdenada;
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
        id: param_RecordLogID
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

        var NameFile = Name_File() + '.xls';
        // Crea el archivo
        var file = fileModulo.create({
          name: NameFile,
          fileType: fileModulo.Type.EXCEL,
          contents: strRetenciones,
          //  encoding: fileModulo.Encoding.UTF8,
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

        log.debug('url', urlfile);
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
            id: param_RecordLogID
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
          //Multibook
          record.setValue({
            fieldId: 'custrecord_lmry_br_rg_multibook',
            value: multibookName
          });

          var recordId = record.save();
          libreria.sendrptuserTranslate(NameReport, 3, NameFile, language);
        }
      } else {
        log.debug('Creacion de File:', 'No existe el folder');
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

      var savedsearch = search.create({
        type: 'customrecord_lmry_br_setup_rpt_dctf',
        columns: [
          //00 Latam - BR Forma Tributaria Code
          search.createColumn({
            name: "custrecord_lmry_br_code_taxation_profit",
            join: "CUSTRECORD_LMRY_BR_FORMA_TRIBUTACION",
            label: "Latam - BR Forma Tributaria Code"
          }),
          //01 Latam - BR Account Payroll Internal ID
          search.createColumn({
            name: "internalid",
            join: "CUSTRECORD_LMRY_BR_ACCOUNT_PAYROLL",
            label: " Latam - BR Account Payroll Internal ID"
          }),
          //02 LATAM - BR ID TRIBUTE PAYROLL
          search.createColumn({
            name: "custrecord_lmry_br_id_tribute_payroll",
            label: "Latam - BR Id Tribute PayRoll"
          }),
          //03 LATAM - BR ID RECEITA PAYROLL
          search.createColumn({
            name: "custrecord_lmry_br_id_receita_payroll",
            label: "Latam - BR Id Receita PayRoll"
          }),
          //04 LATAM - BR ID PERIODICITY PAYROLL
          search.createColumn({
            name: "custrecord_lmry_br_id_periodicity_payrol",
            label: "Latam - BR Id Periodicity PayRoll"
          }),
          //05 LATAM - BR LIMITE AMOUNT IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_monto_limite_irpj",
            label: "Latam - BR Limit Amount IRPJ"
          }),
          //06 LATAM - BR PORC ALICUOTA IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_porc_alicuota_irpj",
            label: "Latam - BR Porc Alicuota IRPJ"
          }),
          //07 LATAM - BR PORC ADICIONAL IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_porc_adicional_irpj",
            label: "Latam - BR Porc Adicional IRPJ"
          }),
          //08 LATAM - BR PORC ALICUOTA CSLL
          search.createColumn({
            name: "custrecord_lmry_br_porc_alicuota_csll",
            label: "Latam - BR Porc Alicuota CSLL"
          }),
          //09 LATAM - BR ID RECEITA IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_id_receita_irpj",
            label: "Latam - BR Id Receita IRPJ"
          }),
          //10 LATAM - BR ID PERIODICITY IRPJ
          search.createColumn({
            name: "custrecord_lmry_br_id_periodicity_irpj",
            label: "Latam - BR Id Periodicity IRPJ"
          }),
          //11 LATAM - BR ID RECEITA CSLL
          search.createColumn({
            name: "custrecord_lmry_br_id_receita_csll",
            label: "Latam - BR Id Receita CSLL"
          }),
          //12 LATAM - BR ID PERIODICITY CSLL
          search.createColumn({
            name: "custrecord_lmry_br_id_periodicity_csll",
            label: "Latam - BR Id Periodicity CSLL"
          }),
          //13 LATAM - BR LEGAL REPRESENTATIVE SERVICES
          search.createColumn({
            name: "internalid",
            join: "CUSTRECORD_LMRY_BR_SERV_REPR_LEGAL",
            label: "LATAM - BR LEGAL REPRESENTATIVE SERVICES"
          }),
          //14 Latam - BR ID TRIBUTE WHT - IRRF -Representante
          search.createColumn({
            name: "custrecord_lmry_br_id_tribute_legalper",
            label: "Latam - BR ID TRIBUTE WHT - IRRF - Representante"
          }),
          //15 LATAM - BR ID RECEITA WHT - IRRF -Representante
          search.createColumn({
            name: "custrecord_lmry_br_id_receita_legalper",
            label: "LATAM - BR ID RECEITA WHT - IRRF - Representante"
          }),
          //16 LATAM - BR ID PERIODICITY WHT - IRRF -Representante
          search.createColumn({
            name: "custrecord_lmry_br_id_period_lega_person",
            label: "LATAM - BR ID PERIODICITY WHT - IRRF - Representante"
          }),
          //17 Filiales
          search.createColumn({
            name: "custrecord_lmry_br_filiales",
            label: "Filiales"
          }),
          //18
          search.createColumn({
            name: "custrecord_lmry_br_tipo_concepto_asiento",
            label: "Latam - BR Type Concept Journal"
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
      var objResult = searchresult.getRange(0, 1000);

      if (objResult != null && objResult.length != 0) {
        var intLength = objResult.length;

        for (var i = 0; i < intLength; i++) {
          var columns = objResult[i].columns;
          //Forma Tributaria
          cod_form_tribut = objResult[i].getValue(columns[0]);
          //Id Account Payroll
          id_account_payroll = objResult[i].getValue(columns[1]);
          if (i == 0) {
            Var_Acount_Payroll = id_account_payroll;
          } else {
            Var_Acount_Payroll = Var_Acount_Payroll + ',' + id_account_payroll;
          }
          //Id Tributo Payroll
          id_tributo_payroll = objResult[i].getValue(columns[2]);
          //Id Receta Payroll
          id_receta_payroll = objResult[i].getValue(columns[3]);
          //Id Periodicidad Payroll
          id_periodicidad_payroll = objResult[i].getValue(columns[4]);
          //Monto Limite
          monto_limite = objResult[i].getValue(columns[5]);
          //Porc Alicuota IRPJ
          porc_alicuota_irpj = objResult[i].getValue(columns[6]);
          //Porc Adicional IRPJ
          porc_adicional_irpj = objResult[i].getValue(columns[7]);
          //Porc Alicuota IRPJ
          porc_alicuota_csll = objResult[i].getValue(columns[8]);
          //Id Receta IRPJ
          id_receta_irpj = objResult[i].getValue(columns[9]);
          //Id Periodicidad IRPJ
          id_periodicidad_irpj = objResult[i].getValue(columns[10]);
          //ID Receta CSLL
          id_receta_csll = objResult[i].getValue(columns[11]);
          //Periodicidad CSLL
          id_periodicidad_csll = objResult[i].getValue(columns[12]);
          //LATAM - BR LEGAL REPRESENTATIVE SERVICES
          id_item_repres_legal = objResult[i].getValue(columns[13]);
          id_tribute_wht_irrf_repres = objResult[i].getValue(columns[14]);
          id_receita_wht_irrf_repres = objResult[i].getValue(columns[15]);
          periodicidad_irrf_repres = objResult[i].getValue(columns[16]);

          Filiales = objResult[i].getValue(columns[17]);
          Type_concept_journal = objResult[i].getValue(columns[18]);
        }
      } else {
        log.debug('No existe configuracion para la subsidiaria matriz');
      }
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
          //1
          search.createColumn({
            name: "type",
            summary: "GROUP",
            label: "Type"
          }),
          //2
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
        //3. Exchange Rate / Multibook
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
          var arraux = new Array();
          if (feature_Multi) {
            var amount_journal = objResult[i].getValue(columns[3]);
          } else {
            var amount_journal = objResult[i].getValue(columns[0]);
          }
          //0.Monto
          arraux[0] = amount_journal;
          //1.Type
          arraux[1] = objResult[i].getValue(columns[1]);
          ArrPayroll.push(arraux);
        }
      }
      log.debug('ArrPayroll', ArrPayroll);
    }

    function formatEPayLines(arrData) {

      for (var i = 0; i < arrData.length; i++) {
        var arrTemporal = [];
        //0 Document Number
        arrTemporal.push(arrData[i][16]);
        //1 Tipo de Doc
        arrTemporal.push(arrData[i][0]);
        //2 Fecha de Pago
        arrTemporal.push('');
        //3 Fecha de Vencimiento
        arrTemporal.push(arrData[i][11]);
        //4 Tributo
        arrTemporal.push(arrData[i][2]);
        //5 Cod Tributo
        arrTemporal.push(arrData[i][1]);
        //6 Receita
        arrTemporal.push(arrData[i][3]);
        //7 Periodicidad
        arrTemporal.push(arrData[i][4]);
        //8 Valor Principal
        var montoPrincipal = Number(arrData[i][15]).toFixed(2);
        arrTemporal.push(montoPrincipal);
        //9 Valor Multa
        var montoMulta = Number(arrData[i][6]).toFixed(2);
        arrTemporal.push(montoMulta);
        //10 Valor Juros
        var montoJuros = Number(arrData[i][5]).toFixed(2);
        arrTemporal.push(montoJuros);
        //11 Valor Pago
        var total = montoPrincipal + montoMulta + montoJuros;
        arrTemporal.push(total);

        //TIPO DE CONCEPTO
        var concepto = objResult[i].getValue(columns[9]);

        if (concepto == 5) {
          ArrEPayLinesPago.push(arrTemporal);
        } else {
          ArrEPayLinesNomina.push(arrTemporal);
        }

      }
    }

    function ObtenerDataPagosTributos(concepto) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;

      var DbolStop = false;

      var savedsearch = search.load({
        /* LatamReady BR - Tax Payment DCTF */
        id: 'customsearch_lmry_br_dctf_tax_payment'
      });

      //Subsidiaria
      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: SubsidiariasContempladas
        });
        savedsearch.filters.push(subsidiaryFilter);
      }
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
      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
        if (objResult != null) {
          var intLength = objResult.length;

          var vectorJournals = [];
          for (var i = 0; i < intLength; i++) {
            var arrtemporal = [];
            var columns = objResult[i].columns;

            //Document Number
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != '') {
              var columna0 = objResult[i].getValue(columns[1]);
            } else {
              var columna0 = '';
            }
            //Tipo de Doc
            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != '') {
              var columna1 = objResult[i].getValue(columns[2]);
            } else {
              var columna1 = '';
            }
            //Fecha de Pago
            if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != '') {
              var columna2 = objResult[i].getValue(columns[4]);
            } else {
              var columna2 = '';
            }
            //Fecha de Vencimiento
            if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != '') {
              var columna3 = objResult[i].getValue(columns[5]);
            } else {
              var columna3 = '';
            }
            //Tributo
            if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != '') {
              var columna4 = objResult[i].getValue(columns[6]);
            } else {
              var columna4 = '';
            }
            //Cod Tributo
            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != '') {
              var columna5 = objResult[i].getValue(columns[7]);
            } else {
              var columna5 = '';
            }
            //Receita
            if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != '') {
              var columna6 = objResult[i].getValue(columns[8]);
            } else {
              var columna6 = '';
            }
            //Periodicidad
            if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != '') {
              var columna7 = objResult[i].getValue(columns[9]);
            } else {
              var columna7 = '';
            }
            /*Amount Class*/
            var amountClass = objResult[i].getValue(columns[12]);
            /*Monto debit*/
            if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 0) {
              var monto = Number(objResult[i].getValue(columns[10]));
            } else {
              var monto = 0;
            }

            var columna8;
            var columna9;
            var columna10;
            //Valor Principal
            (amountClass == '' || amountClass == null || amountClass == '1') ? columna8 = monto: columna8 = 0.00;
            //Valor Multa
            amountClass == '2' ? columna9 = monto : columna9 = 0.00;
            //Valor Juros
            amountClass == '3' ? columna10 = monto : columna10 = 0.00;
            //Valor Pago
            var columna11 = columna8 + columna9 + columna10;

            /*INTERNAL ID*/
            var columna12 = objResult[i].getValue(columns[0]);
            //TIPO DE CONCEPTO
            var conceptoTrnsac = objResult[i].getValue(columns[3]);

            if (monto != 0 && conceptoTrnsac == concepto) {
              if (columna5 == '04' || columna5 == '09') { //si es imp importacion (CIDE o IOF)
                if (concepto == 5) {
                  if (verificarImportacion(objResult[i].getValue(columns[0]))) { //si el bill de importacion es configurado correctamente
                    arrtemporal = [columna0, columna1, columna2, columna3, columna4, columna5, columna6, columna7, columna8, columna9, columna10, columna11, columna12];
                    vectorJournals.push(arrtemporal);
                  }
                } else {
                  arrtemporal = [columna0, columna1, columna2, columna3, columna4, columna5, columna6, columna7, columna8, columna9, columna10, columna11, columna12];
                  vectorJournals.push(arrtemporal);
                }

              } else {
                arrtemporal = [columna0, columna1, columna2, columna3, columna4, columna5, columna6, columna7, columna8, columna9, columna10, columna11, columna12];
                vectorJournals.push(arrtemporal);
              }
            }

          }

          if (intLength != 1000) {
            DbolStop = true;
          }

          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
        } else {
          DbolStop = true;
        }
      }
      return vectorJournals;
    }

    function agruparR11(arrayData) {
      var arrActualizado = [];
      var long = arrayData.length;
      var i = 0;
      var j = 0;

      while (j < long) {
        var montoPrincipal = Number(arrayData[j][8]);
        var montoMulta = Number(arrayData[j][9]);
        var montoJuros = Number(arrayData[j][10]);
        var montoTotal = Number(arrayData[j][11]);

        i = j + 1;
        while (i < long) { //internalid-tributo-receta
          if (arrayData[j][12] == arrayData[i][12] && arrayData[j][5] == arrayData[i][5] && arrayData[j][6] == arrayData[i][6]) {
            montoPrincipal += Number(arrayData[i][8]);
            montoMulta += Number(arrayData[i][9]);
            montoJuros += Number(arrayData[i][10]);
            montoTotal += Number(arrayData[i][11]);
            arrayData.splice(i, 1);
            long--;
          } else {
            i++;
          }
        }
        arrayData[j][8] = (montoPrincipal).toFixed(2);
        arrayData[j][9] = (montoMulta).toFixed(2);
        arrayData[j][10] = (montoJuros).toFixed(2);
        arrayData[j][11] = (montoTotal).toFixed(2);
        arrayData[j].pop();
        arrActualizado.push(arrayData[j]);
        j++;
      }
      log.debug('arrActualizado R11', arrActualizado);
      return arrActualizado;
    }

    function obtenerJournalsR12_R14(registro) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;

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
        concepto = '22';
      } else if (registro == 'R14') {
        concepto = '21';
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
      var arrayData = [];

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
            //0. # DOC
            arrAuxiliar[0] = objResult[i].getValue(columns[1]);
            //1. FECHA
            arrAuxiliar[1] = objResult[i].getValue(columns[2]);
            //2. NOMBRE DE TRIBUTO
            arrAuxiliar[2] = objResult[i].getValue(columns[3]);
            //3. CODIGO DE TRIBUTO
            arrAuxiliar[3] = objResult[i].getValue(columns[4]);
            //4. CODIGO DE RECETA
            arrAuxiliar[4] = objResult[i].getValue(columns[5]);
            //5. PERIODICIDAD
            arrAuxiliar[5] = objResult[i].getValue(columns[6]);
            //6. DEBIT AMOUNT
            if (feature_Multi) {
              arrAuxiliar[6] = objResult[i].getValue(columns[13]);
            } else {
              arrAuxiliar[6] = objResult[i].getValue(columns[7]);
            }
            if (registro == 'R14') {
              /* AMOUNT CLASS */
              var amountClass = objResult[i].getValue(columns[12]);
              //7. VALOR PRINCIPAL
              (amountClass == '' || amountClass == null || amountClass == '1') ? arrAuxiliar[7] = arrAuxiliar[6]: arrAuxiliar[7] = 0.00;
              //8. VALOR MULTA
              amountClass == '2' ? arrAuxiliar[8] = arrAuxiliar[6] : arrAuxiliar[8] = 0.00;
              //9. VALOR JUROS
              amountClass == '3' ? arrAuxiliar[9] = arrAuxiliar[6] : arrAuxiliar[9] = 0.00;
              //10. internal id
              arrAuxiliar[10] = objResult[i].getValue(columns[0]);
            }

            if (arrAuxiliar[6] != 0) {
              arrayData.push(arrAuxiliar);
            }
          }
          log.error('Journals ' + registro, arrayData);
          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
        } else {
          DbolStop = true;
        }
      }

      return arrayData;
    }

    function agruparR14(arrayData) {
      var arrActualizado = [];
      var long = arrayData.length;
      var i = 0;
      var j = 0;

      while (j < long) {
        var montoSuspension = Number(arrayData[j][6]);
        var montoPrincipal = Number(arrayData[j][7]);
        var montoMulta = Number(arrayData[j][8]);
        var montoJuros = Number(arrayData[j][9]);

        i = j + 1;
        while (i < long) { //internalid-tributo-receta
          if (arrayData[j][10] == arrayData[i][10] && arrayData[j][3] == arrayData[i][3] && arrayData[j][4] == arrayData[i][4]) {
            montoSuspension += Number(arrayData[i][6]);
            montoPrincipal += Number(arrayData[i][7]);
            montoMulta += Number(arrayData[i][8]);
            montoJuros += Number(arrayData[i][9]);
            arrayData.splice(i, 1);
            long--;
          } else {
            i++;
          }
        }
        arrayData[j][6] = (montoSuspension).toFixed(2);
        arrayData[j][7] = (montoPrincipal).toFixed(2);
        arrayData[j][8] = (montoMulta).toFixed(2);
        arrayData[j][9] = (montoJuros).toFixed(2);
        arrayData[j].pop();
        arrActualizado.push(arrayData[j]);
        j++;
      }
      log.debug('arrActualizado R14', arrActualizado);
      return arrActualizado;
    }

    function verificarImportacion(idJournal) {
      var resultado = false;
      for (var i = 0; i < ArrJournalImportacionTemp.length; i++) {
        if (ArrJournalImportacionTemp[i][7] == idJournal) {
          resultado = true;
          break;
        }
      }

      return resultado;
    }

    function obtenerVectorJournalTributo(tributo, receita, periodicidad) {
      //Esta enviando como parametros tributos que existen y en caso exista en el vector
      // de journals cargados, procedera  a cargar  un nuevo vector de Journasl conincidentes
      var long = VectorPagosTributos.length;
      var i = 0;
      while (i < long) {
        if (VectorPagosTributos[i][5] == tributo && VectorPagosTributos[i][6] == receita && VectorPagosTributos[i][7] == periodicidad) {
          JournalsCoinciR11.push(VectorPagosTributos[i]);
          VectorPagosTributos.splice(i, 1);
          long--;
        } else {
          i++;
        }
      }
    }

    function obtenerR12_R14Journal(tributo, receita, periodicidad, arrayData, arrayResult) {
      var long = arrayData.length;
      var i = 0;
      while (i < long) {
        if (arrayData[i][3] == tributo && arrayData[i][4] == receita && arrayData[i][5] == periodicidad) {
          arrayResult.push(arrayData[i]);
          arrayData.splice(i, 1);
          long--;
        } else {
          i++;
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
          }),
          search.createColumn({
            name: "type",
            summary: "GROUP",
            label: "Type"
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
          var arraux = new Array();
          if (feature_Multi) {
            var amount_journal = objResult[i].getValue(columns[3]);
          } else {
            var amount_journal = objResult[i].getValue(columns[0]);
          }
          //0.Monto
          arraux[0] = amount_journal;
          //1.Type
          arraux[1] = objResult[i].getValue(columns[1]);
          ArrPayrollCPRB.push(arraux);
        }
      }
      log.debug('ArrPayrollCPRB', ArrPayrollCPRB);
    }

    function CargarCabeceraEstilosExcel() {
      //Crear Excel
      strRetenciones_aux += '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
      strRetenciones_aux += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
      strRetenciones_aux += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
      strRetenciones_aux += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
      strRetenciones_aux += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
      strRetenciones_aux += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
      //Estilos de Celdas
      strRetenciones_aux += '<Styles>';

      strRetenciones_aux += '<Style ss:ID="s20">'; // estilo cabecera de tabla
      strRetenciones_aux += '<Alignment ss:Horizontal="Center" ss:Vertical="Center"/>';
      strRetenciones_aux += '<Interior ss:Color="#e58f29" ss:Pattern="Solid"/>';
      strRetenciones_aux += '<Font ss:FontName="Arial" x:Family="Swiss" ss:Size="12" ss:Bold="1"/>';
      strRetenciones_aux += '<Borders>';
      strRetenciones_aux += '<Border ss:Position="Bottom" ss:LineStyle="Continuous" ss:Weight="1"/>';
      strRetenciones_aux += '<Border ss:Position="Left" ss:LineStyle="Continuous" ss:Weight="1"/>';
      strRetenciones_aux += '<Border ss:Position="Right" ss:LineStyle="Continuous" ss:Weight="1"/>';
      strRetenciones_aux += '<Border ss:Position="Top" ss:LineStyle="Continuous" ss:Weight="1"/>';
      strRetenciones_aux += '</Borders>';
      strRetenciones_aux += '</Style>';

      strRetenciones_aux += '<Style ss:ID="s26">'; // estilo Subcabecera de tabla
      strRetenciones_aux += '<Alignment ss:Horizontal="Center" ss:Vertical="Center"/>';
      strRetenciones_aux += '<Interior ss:Color="#e5d029" ss:Pattern="Solid"/>';
      strRetenciones_aux += '<Font ss:FontName="Arial" x:Family="Swiss" ss:Size="12" ss:Bold="1"/>';
      strRetenciones_aux += '</Style>';

      strRetenciones_aux += '<Style ss:ID="s25">'; // estilo para datos de tabla
      strRetenciones_aux += '<Interior ss:Color="#FFFFFF" ss:Pattern="Solid"/>';
      strRetenciones_aux += '<Alignment ss:Vertical="Center"/>';
      strRetenciones_aux += '<Borders>';
      strRetenciones_aux += '<Border ss:Position="Bottom" ss:LineStyle="Continuous" ss:Weight="1"/>';
      strRetenciones_aux += '<Border ss:Position="Left" ss:LineStyle="Continuous" ss:Weight="1"/>';
      strRetenciones_aux += '<Border ss:Position="Right" ss:LineStyle="Continuous" ss:Weight="1"/>';
      strRetenciones_aux += '<Border ss:Position="Top" ss:LineStyle="Continuous" ss:Weight="1"/>';
      strRetenciones_aux += '</Borders>';
      strRetenciones_aux += '</Style>';

      strRetenciones_aux += '<Style ss:ID="s22">'; //estilo celdas vacias y titulo
      strRetenciones_aux += '<Alignment ss:Horizontal="Center" ss:Vertical="Center"/>';
      strRetenciones_aux += '<Font x:Family="Swiss" ss:Size="16" ss:Bold="1"/>';
      strRetenciones_aux += '</Style>';

      strRetenciones_aux += '<Style ss:ID="s23">'; // estilo para datos fijos
      strRetenciones_aux += '<Alignment ss:Vertical="Center"/>';
      strRetenciones_aux += '</Style>';

      strRetenciones_aux += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';

      strRetenciones_aux += '</Styles>';
    }

    function CargarTituloHojaExcel(nroEspacio, titulo) {
      var strtituloExcel = '';
      for (var i = 0; i < 20; i++) {
        strtituloExcel += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      }
      //TITULO
      strtituloExcel += '<Row>';
      for (var i = 0; i < nroEspacio; i++) {
        strtituloExcel += '<Cell/>';
      }
      strtituloExcel += '<Cell ss:StyleID="s22" ><Data ss:Type="String">DCTF </Data></Cell>';
      strtituloExcel += '</Row>';

      strtituloExcel += '<Row>';
      for (var i = 0; i < nroEspacio - 1; i++) {
        strtituloExcel += '<Cell/>';
      }
      strtituloExcel += '<Cell ss:MergeAcross="2" ss:StyleID="s22" ><Data ss:Type="String">' + titulo + '</Data></Cell>';
      strtituloExcel += '</Row>';

      strtituloExcel += '<Row/>';
      //DATOS FIJOS
      strtituloExcel += '<Row>';
      for (var i = 0; i < nroEspacio - 1; i++) {
        strtituloExcel += '<Cell/>';
      }
      strtituloExcel += '<Cell ss:StyleID="s23" ><Data ss:Type="String">CNPJ:</Data></Cell>';
      strtituloExcel += '<Cell ss:StyleID="s23" ><Data ss:Type="String">' + companyruc + '</Data></Cell>';
      strtituloExcel += '</Row>';

      strtituloExcel += '<Row>';
      for (var i = 0; i < nroEspacio - 1; i++) {
        strtituloExcel += '<Cell/>';
      }
      strtituloExcel += '<Cell ss:StyleID="s23" ><Data ss:Type="String">Periodo:</Data></Cell>';
      strtituloExcel += '<Cell ss:StyleID="s23" ><Data ss:Type="String">' + periodname + '</Data></Cell>';
      strtituloExcel += '</Row>';

      strtituloExcel += '<Row/><Row/>';
      return strtituloExcel;
    }

    function CargaDatosDebCredRealEstimativo() {
      var pis_estado = false;
      var cofins_estado = false;
      var agregarSeparador = true;

      strRetenciones_aux += ' <Worksheet ss:Name="Impuestos a Pagar">';
      strRetenciones_aux += '<Table>';
      strRetenciones_aux += CargarTituloHojaExcel(3, 'Reporte Auditoria de Impuestos adeudados');
      //Declaracion de las columnas
      strRetenciones_aux += '<Row>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> # DOC </Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> TIPO DOC </Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> TRIBUTO </Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> COD. TRIBUTO </Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> RECEITA </Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> PERIODICIDAD</Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR DE DEBITO </Data></Cell>' +
        '</Row>';

      /********************************* CARGAR SALES ***********************************************/
      percentage_receta = Number(validarCaracteres(percentage_receta)) / 100;

      for (var i = 0; i < ArrSetupDCTF_Sales.length; i++) {
        if (ArrSetupDCTF_Sales[i][1] == '07' || ArrSetupDCTF_Sales[i][1] == '06') {
          if (agregarSeparador) {
            colocarSeparador("VENTAS");
            agregarSeparador = false;
          }
          strRetenciones_aux += '<Row>';
          //NUMERO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][9] + '</Data></Cell>';
          //TIPO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][0] + '</Data></Cell>';
          //Tributo
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][2] + '</Data></Cell>';
          //codigo de TRIBUTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][1] + '</Data></Cell>';
          //receita1
          if (ArrSetupDCTF_Sales[i][3] != null && ArrSetupDCTF_Sales[i][3] != '' && ArrSetupDCTF_Sales[i][3] != '- None -') {
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][3] + '</Data></Cell>';
          } else {
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + '' + '</Data></Cell>';
          }
          //PERIODICIDAD
          if (ArrSetupDCTF_Sales[i][4] != null && ArrSetupDCTF_Sales[i][4] != '' && ArrSetupDCTF_Sales[i][4] != '- None -') {
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][4] + '</Data></Cell>';
          } else {
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + '' + '</Data></Cell>';
          }
          //MONTO
          monto = Number(ArrSetupDCTF_Sales[i][5]).toFixed(2);
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

          strRetenciones_aux += '</Row>';
          obtenerVectorJournalTributo(ArrSetupDCTF_Sales[i][1], ArrSetupDCTF_Sales[i][3], ArrSetupDCTF_Sales[i][4]);
          obtenerR12_R14Journal(ArrSetupDCTF_Sales[i][1], ArrSetupDCTF_Sales[i][3], ArrSetupDCTF_Sales[i][4], ArrJournalR12, LineasR12);
          obtenerR12_R14Journal(ArrSetupDCTF_Sales[i][1], ArrSetupDCTF_Sales[i][3], ArrSetupDCTF_Sales[i][4], ArrJournalR14, LineasR14);
        }
      }

      agregarSeparador = true;
      for (var i = 0; i < ArrSetupDCTF_Sales_Inv.length; i++) {
        if (ArrSetupDCTF_Sales_Inv[i][1] == '06' || ArrSetupDCTF_Sales_Inv[i][1] == '07' || ArrSetupDCTF_Sales_Inv[i][1] == '03') {
          if (agregarSeparador) {
            colocarSeparador("VENTAS INVENTARIO");
            agregarSeparador = false;
          }
          strRetenciones_aux += '<Row>';
          //NUMERO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][9] + '</Data></Cell>';
          //TIPO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][0] + '</Data></Cell>';
          //Tributo
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][2] + '</Data></Cell>';
          //codigo de TRIBUTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][1] + '</Data></Cell>';
          //receita1
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][3] + '</Data></Cell>';
          //PERIODICIDAD
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][4] + '</Data></Cell>';
          //MONTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + Number(ArrSetupDCTF_Sales_Inv[i][5]).toFixed(2) + '</Data></Cell>';

          strRetenciones_aux += '</Row>';
          obtenerVectorJournalTributo(ArrSetupDCTF_Sales_Inv[i][1], ArrSetupDCTF_Sales_Inv[i][3], ArrSetupDCTF_Sales_Inv[i][4]);
          obtenerR12_R14Journal(ArrSetupDCTF_Sales_Inv[i][1], ArrSetupDCTF_Sales_Inv[i][3], ArrSetupDCTF_Sales_Inv[i][4], ArrJournalR12, LineasR12);
          obtenerR12_R14Journal(ArrSetupDCTF_Sales_Inv[i][1], ArrSetupDCTF_Sales_Inv[i][3], ArrSetupDCTF_Sales_Inv[i][4], ArrJournalR14, LineasR14);
        }
      }
      /************************************* CARGAR PURCHASES *****************************************************/
      agregarSeparador = true;
      for (var i = 0; i < ArrSetupDCTF_Purchases.length; i++) {
        if (ArrSetupDCTF_Purchases[i][1] == '07' || ArrSetupDCTF_Purchases[i][1] == '06') {
          if (agregarSeparador) {
            colocarSeparador("COMPRAS");
            agregarSeparador = false;
          }
          strRetenciones_aux += '<Row>';
          //NUMERO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases[i][9] + '</Data></Cell>';
          //TIPO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases[i][0] + '</Data></Cell>';
          //Tributo
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases[i][2] + '</Data></Cell>';
          //codigo de TRIBUTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases[i][1] + '</Data></Cell>';
          //receita1
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases[i][3] + '</Data></Cell>';
          //PERIODICIDAD
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases[i][4] + '</Data></Cell>';
          //MONTO
          monto = Number(ArrSetupDCTF_Purchases[i][5]).toFixed(2);
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

          strRetenciones_aux += '</Row>';
          obtenerVectorJournalTributo(ArrSetupDCTF_Purchases[i][1], ArrSetupDCTF_Purchases[i][3], ArrSetupDCTF_Purchases[i][4]);
          obtenerR12_R14Journal(ArrSetupDCTF_Purchases[i][1], ArrSetupDCTF_Purchases[i][3], ArrSetupDCTF_Purchases[i][4], ArrJournalR12, LineasR12);
          obtenerR12_R14Journal(ArrSetupDCTF_Purchases[i][1], ArrSetupDCTF_Purchases[i][3], ArrSetupDCTF_Purchases[i][4], ArrJournalR14, LineasR14);
        }
      }

      agregarSeparador = true;
      for (var i = 0; i < ArrSetupDCTF_Purchases_Inv.length; i++) {
        if (ArrSetupDCTF_Purchases_Inv[i][1] == '06' || ArrSetupDCTF_Purchases_Inv[i][1] == '07' || ArrSetupDCTF_Purchases_Inv[i][1] == '03') {
          if (agregarSeparador) {
            colocarSeparador("COMPRAS INVENTARIO");
            agregarSeparador = false;
          }

          strRetenciones_aux += '<Row>';
          //NUMERO DOC
          if (ArrSetupDCTF_Purchases_Inv[i][9] != '- None -') {
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][9] + '</Data></Cell>';
          } else {
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">-</Data></Cell>';

          }
          //TIPO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][0] + '</Data></Cell>';
          //Tributo
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][2] + '</Data></Cell>';
          //codigo de TRIBUTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][1] + '</Data></Cell>';
          //receita1
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][3] + '</Data></Cell>';
          //PERIODICIDAD
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][4] + '</Data></Cell>';
          //MONTO
          monto = ArrSetupDCTF_Purchases_Inv[i][5];
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + Number(ArrSetupDCTF_Purchases_Inv[i][5]).toFixed(2) + '</Data></Cell>';

          strRetenciones_aux += '</Row>';
          obtenerVectorJournalTributo(ArrSetupDCTF_Purchases_Inv[i][1], ArrSetupDCTF_Purchases_Inv[i][3], ArrSetupDCTF_Purchases_Inv[i][4]);
          obtenerR12_R14Journal(ArrSetupDCTF_Purchases_Inv[i][1], ArrSetupDCTF_Purchases_Inv[i][3], ArrSetupDCTF_Purchases_Inv[i][4], ArrJournalR12, LineasR12);
          obtenerR12_R14Journal(ArrSetupDCTF_Purchases_Inv[i][1], ArrSetupDCTF_Purchases_Inv[i][3], ArrSetupDCTF_Purchases_Inv[i][4], ArrJournalR14, LineasR14);
        }
      }

      var importacionesTotal = ArrJournalImportacion.concat(ArrIOFBillPay, ArrCIDEBillPay);
      agregarLineasImportacion(importacionesTotal);

      //CARGAR IRPJ
      if (param_Lucro_Conta != 0) {
        colocarSeparador("CÁLCULOS");

        strRetenciones_aux += '<Row>';
        //NUMERO DOC
        strRetenciones_aux += '<Cell ss:StyleID="s25"><</Cell>';
        //TIPO DOC
        strRetenciones_aux += '<Cell ss:StyleID="s25"></Cell>';
        //Tributo
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">IRPJ</Data></Cell>';
        //codigo de TRIBUTO
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">01</Data></Cell>';
        //receita1
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_receta_irpj + '</Data></Cell>';
        //PERIODICIDAD
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_periodicidad_irpj + '</Data></Cell>';
        //MONTO
        monto = Number(param_Lucro_Conta) + Number(param_Monto_Adicional) - Number(param_Monto_Excluyente);
        porc_alicuota_irpj = Number(validarCaracteres(porc_alicuota_irpj)) / 100;
        porc_adicional_irpj = Number(validarCaracteres(porc_adicional_irpj)) / 100;
        if (monto < Number(monto_limite)) {
          monto = (monto * porc_alicuota_irpj);
        } else {
          monto = (monto * porc_alicuota_irpj + (monto - (Number(monto_limite) * Number(mes_date))) * porc_adicional_irpj);
        }
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

        strRetenciones_aux += '</Row>';
        obtenerVectorJournalTributo('01', id_receta_irpj, id_periodicidad_irpj);
        obtenerR12_R14Journal('01', id_receta_irpj, id_periodicidad_irpj, ArrJournalR12, LineasR12);
        obtenerR12_R14Journal('01', id_receta_irpj, id_periodicidad_irpj, ArrJournalR14, LineasR14);
      }
      //CARGAR CSLL
      if (param_Lucro_Conta != 0) {
        strRetenciones_aux += '<Row>';
        //NUMERO DOC
        strRetenciones_aux += '<Cell ss:StyleID="s25"><</Cell>';
        //TIPO DOC
        strRetenciones_aux += '<Cell ss:StyleID="s25"></Cell>';
        //Tributo
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">CSLL</Data></Cell>';
        //codigo de TRIBUTO
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">05</Data></Cell>';
        //receita1
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_receta_csll + '</Data></Cell>';
        //PERIODICIDAD
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_periodicidad_csll + '</Data></Cell>';
        //MONTO
        monto = Number(param_Lucro_Conta) + Number(param_Monto_Adicional) - Number(param_Monto_Excluyente);
        porc_alicuota_csll = Number(validarCaracteres(porc_alicuota_csll)) / 100;
        monto = (monto * porc_alicuota_csll);
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

        strRetenciones_aux += '</Row>';
        obtenerVectorJournalTributo('05', id_receta_csll, id_periodicidad_csll);
        obtenerR12_R14Journal('05', id_receta_csll, id_periodicidad_csll, ArrJournalR12, LineasR12);
        obtenerR12_R14Journal('05', id_receta_csll, id_periodicidad_csll, ArrJournalR14, LineasR14);
      }

      /*================================================= RETENCIONES ========================================================================*/
      agregarSeparador = true;
      for (var j = 0; j < Arr_WHT_No_Item.length; j++) {
        if (Arr_WHT_No_Item[j][0] == '02' || Arr_WHT_No_Item[j][0] == '11') {
          if (agregarSeparador) {
            colocarSeparador("RETENCIONES");
            agregarSeparador = false;
          }
          strRetenciones_aux += '<Row>';
          //NUMERO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + Arr_WHT_No_Item[j][6] + '</Data></Cell>';
          //TIPO DOC
          //Tributo
          if (Arr_WHT_No_Item[j][0] == '02') {
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String"> Bill </Data></Cell>';
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">IRRF</Data></Cell>';
          }
          if (Arr_WHT_No_Item[j][0] == '11') {
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String"> Bill Payment </Data></Cell>';
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">CSRF</Data></Cell>';
          }
          //codigo de TRIBUTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + Arr_WHT_No_Item[j][0] + '</Data></Cell>';
          //receita1
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + Arr_WHT_No_Item[j][3] + '</Data></Cell>';
          //PERIODICIDAD
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + Arr_WHT_No_Item[j][5] + '</Data></Cell>';
          //MONTO
          monto = Number(Arr_WHT_No_Item[j][4]);
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

          strRetenciones_aux += '</Row>';
          obtenerVectorJournalTributo(Arr_WHT_No_Item[j][0], Arr_WHT_No_Item[j][3], Arr_WHT_No_Item[j][5]);
          obtenerR12_R14Journal(Arr_WHT_No_Item[j][0], Arr_WHT_No_Item[j][3], Arr_WHT_No_Item[j][5], ArrJournalR12, LineasR12);
          obtenerR12_R14Journal(Arr_WHT_No_Item[j][0], Arr_WHT_No_Item[j][3], Arr_WHT_No_Item[j][5], ArrJournalR14, LineasR14);
        }
      }

      agregarSeparador = true;
      for (var j = 0; j < Arr_WHT_Item.length; j++) {
        if (Arr_WHT_Item[j][0] == '02') {
          if (agregarSeparador) {
            colocarSeparador("RETENCIÓN REPRESENTANTE LEGAL");
            agregarSeparador = false;
          }
          strRetenciones_aux += '<Row>';
          //NUMERO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + Arr_WHT_Item[j][6] + '</Data></Cell>';
          //TIPO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">Bill</Data></Cell>';
          //Tributo
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">IRRF</Data></Cell>';
          //codigo de TRIBUTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_tribute_wht_irrf_repres + '</Data></Cell>';
          //receita1
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_receita_wht_irrf_repres + '</Data></Cell>';
          //PERIODICIDAD
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + periodicidad_irrf_repres + '</Data></Cell>';
          //MONTO
          monto = Arr_WHT_Item[j][4];
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

          strRetenciones_aux += '</Row>';
          obtenerVectorJournalTributo(Arr_WHT_Item[j][0], Arr_WHT_Item[j][3], Arr_WHT_Item[j][5]);
          obtenerR12_R14Journal(Arr_WHT_Item[j][0], Arr_WHT_Item[j][3], Arr_WHT_Item[j][5], ArrJournalR12, LineasR12);
          obtenerR12_R14Journal(Arr_WHT_Item[j][0], Arr_WHT_Item[j][3], Arr_WHT_Item[j][5], ArrJournalR14, LineasR14);
        }
      }

      /************************************** CARGAR PAYROLL *********************************************/
      if (Type_concept_journal == '1') {
        if (ArrPayroll.length != 0) {
          colocarSeparador("RETENCIÓN PAYROLL");
          for (var i = 0; i < ArrPayroll.length; i++) {
            strRetenciones_aux += '<Row>';
            //NUMERO DOC
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ' ' + '</Data></Cell>';
            //TIPO DOC
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrPayroll[i][1] + '</Data></Cell>';
            //Tributo
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">IRRF</Data></Cell>';
            //codigo de TRIBUTO
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_tributo_payroll + '</Data></Cell>';
            //receita1
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_receta_payroll + '</Data></Cell>';
            //PERIODICIDAD
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_periodicidad_payroll + '</Data></Cell>';
            //MONTO
            monto = ArrPayroll[i][0];
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

            strRetenciones_aux += '</Row>';
            obtenerVectorJournalTributo(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll);
            obtenerR12_R14Journal(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll, ArrJournalR12, LineasR12);
            obtenerR12_R14Journal(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll, ArrJournalR14, LineasR14);
          }
        }
      } else {
        if (ArrPayrollCPRB.length != 0) {
          colocarSeparador("RETENCIÓN PAYROLL");
          for (var i = 0; i < ArrPayrollCPRB.length; i++) {
            strRetenciones_aux += '<Row>';
            //NUMERO DOC
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ' ' + '</Data></Cell>';
            //TIPO DOC
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrPayrollCPRB[i][1] + '</Data></Cell>';
            //Tributo
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">CPSS</Data></Cell>';
            //codigo de TRIBUTO
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_tributo_payroll + '</Data></Cell>';
            //receita1
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_receta_payroll + '</Data></Cell>';
            //PERIODICIDAD
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_periodicidad_payroll + '</Data></Cell>';
            //MONTO
            monto = ArrPayrollCPRB[i][0];
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

            strRetenciones_aux += '</Row>';
            obtenerVectorJournalTributo(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll);
            obtenerR12_R14Journal(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll, ArrJournalR12, LineasR12);
            obtenerR12_R14Journal(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll, ArrJournalR14, LineasR14);
          }
        }
      }
      /************************************** CARGAR IMPOSTOS DE NOMINA *********************************************/
      if (ArrLineasNomina.length != 0) {
        colocarSeparador("IMPUESTOS SOBRE LA NÓMINA");
        for (var i = 0; i < ArrLineasNomina.length; i++) {
          strRetenciones_aux += '<Row>';
          //NUMERO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][0] + '</Data></Cell>';
          //TIPO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][1] + '</Data></Cell>';
          //Tributo
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][4] + '</Data></Cell>';
          //codigo de TRIBUTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][5] + '</Data></Cell>';
          //receita1
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][6] + '</Data></Cell>';
          //PERIODICIDAD
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][7] + '</Data></Cell>';
          //MONTO
          monto = ArrLineasNomina[i][11];
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

          strRetenciones_aux += '</Row>';
        }
      }

      strRetenciones_aux += '</Table></Worksheet>';
      //Entra como parametro vector de Journals q coincide con los tributos y recetas
      var arrPagos = JournalsCoinciR11.concat(ArrLineasNomina);
      strRetenciones_aux += GenerarHojaPagosJournal(arrPagos);
      log.debug('LineasR12', LineasR12);
      strRetenciones_aux += GenerarHojaCompensacionesJournal(LineasR12);
      log.debug('LineasR14', LineasR14);
      strRetenciones_aux += GenerarHojaSuspensionesJournal(LineasR14);
      strRetenciones_aux += '</Workbook>';

      strRetenciones = encode.convert({
        string: strRetenciones_aux,
        inputEncoding: encode.Encoding.UTF_8,
        outputEncoding: encode.Encoding.BASE_64
      });

    }

    function colocarSeparador(mensaje) {
      strRetenciones_aux += '<Row>';
      strRetenciones_aux += '<Cell ss:MergeAcross="6" ss:StyleID="s26"><Data ss:Type="String">' + mensaje + '</Data></Cell>';
      strRetenciones_aux += '</Row>'
    }

    function CargaDatosDebCredPresumido() {
      var monto;
      var agregarSeparador = true;

      strRetenciones_aux += ' <Worksheet ss:Name="Impuestos a Pagar">';
      //declaracion de las columnas
      strRetenciones_aux += '<Table>';

      for (var i = 0; i < 20; i++) {
        strRetenciones_aux += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      }
      strRetenciones_aux += CargarTituloHojaExcel(3, 'Reporte Auditoria de Impuestos adeudados');
      strRetenciones_aux += '<Row>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> # DOC </Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> TIPO DOC </Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> TRIBUTO </Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> COD. TRIBUTO </Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> RECEITA </Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> PERIODICIDAD</Data></Cell>' +
        '<Cell ss:StyleID="s20"><Data ss:Type="String"> VALOR DE DEBITO </Data></Cell>' +
        '</Row>';

      percentage_receta = Number(validarCaracteres(percentage_receta)) / 100;

      ///CARGAR SALES
      for (var i = 0; i < ArrSetupDCTF_Sales.length; i++) {
        if (agregarSeparador) {
          colocarSeparador("VENTAS");
          agregarSeparador = false;
        }
        strRetenciones_aux += '<Row>';
        //NUMERO DOC
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][9] + '</Data></Cell>';
        //TIPO DOC
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][0] + '</Data></Cell>';
        //Tributo
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][2] + '</Data></Cell>';
        //codigo de TRIBUTO
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][1] + '</Data></Cell>';
        //receita1
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][3] + '</Data></Cell>';
        //PERIODICIDAD
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales[i][4] + '</Data></Cell>';
        //MONTO
        monto = ArrSetupDCTF_Sales[i][5];
        if (ArrSetupDCTF_Sales[i][1] == '01' || ArrSetupDCTF_Sales[i][1] == '05') {
          monto = Number(percentage_receta) * Number(monto);
        }
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + Number(monto).toFixed(2) + '</Data></Cell>';

        strRetenciones_aux += '</Row>';
        obtenerVectorJournalTributo(ArrSetupDCTF_Sales[i][1], ArrSetupDCTF_Sales[i][3], ArrSetupDCTF_Sales[i][4]);
        obtenerR12_R14Journal(ArrSetupDCTF_Sales[i][1], ArrSetupDCTF_Sales[i][3], ArrSetupDCTF_Sales[i][4], ArrJournalR12, LineasR12);
        obtenerR12_R14Journal(ArrSetupDCTF_Sales[i][1], ArrSetupDCTF_Sales[i][3], ArrSetupDCTF_Sales[i][4], ArrJournalR14, LineasR14);
      }

      agregarSeparador = true;
      for (var i = 0; i < ArrSetupDCTF_Sales_Inv.length; i++) {
        if (agregarSeparador) {
          colocarSeparador("VENTAS INVENTARIO");
          agregarSeparador = false;
        }
        strRetenciones_aux += '<Row>';
        //NUMERO DOC
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][9] + '</Data></Cell>';
        //TIPO DOC
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][0] + '</Data></Cell>';
        //Tributo
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][2] + '</Data></Cell>';
        //codigo de TRIBUTO
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][1] + '</Data></Cell>';
        //receita1
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][3] + '</Data></Cell>';
        //PERIODICIDAD
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Sales_Inv[i][4] + '</Data></Cell>';
        //MONTO
        monto = ArrSetupDCTF_Sales_Inv[i][5];
        if (ArrSetupDCTF_Sales_Inv[i][1] == '01' || ArrSetupDCTF_Sales_Inv[i][1] == '05') {
          monto = Number(percentage_receta) * Number(monto);
        }
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + Number(monto).toFixed(2) + '</Data></Cell>';
        strRetenciones_aux += '</Row>';
        obtenerVectorJournalTributo(ArrSetupDCTF_Sales_Inv[i][1], ArrSetupDCTF_Sales_Inv[i][3], ArrSetupDCTF_Sales_Inv[i][4]);
        obtenerR12_R14Journal(ArrSetupDCTF_Sales_Inv[i][1], ArrSetupDCTF_Sales_Inv[i][3], ArrSetupDCTF_Sales_Inv[i][4], ArrJournalR12, LineasR12);
        obtenerR12_R14Journal(ArrSetupDCTF_Sales_Inv[i][1], ArrSetupDCTF_Sales_Inv[i][3], ArrSetupDCTF_Sales_Inv[i][4], ArrJournalR14, LineasR14);
      }
      //log.debug('veamos que tiene ', ArrSetupDCTF_Purchases_Inv);
      agregarSeparador = true;
      for (var i = 0; i < ArrSetupDCTF_Purchases_Inv.length; i++) {
        for (var j = 0; j < arrRecetasVentas.length; j++) {
          if (ArrSetupDCTF_Purchases_Inv[i][3] == arrRecetasVentas[j]) {
            if (ArrSetupDCTF_Purchases_Inv[i][1] == '03') {
              if (agregarSeparador) {
                colocarSeparador("COMPRAS INVENTARIO");
                agregarSeparador = false;
              }
              strRetenciones_aux += '<Row>';
              //NUMERO DOC
              strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][9] + '</Data></Cell>';
              //TIPO DOC
              strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][0] + '</Data></Cell>';
              //Tributo
              strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][2] + '</Data></Cell>';
              //codigo de TRIBUTO
              strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][1] + '</Data></Cell>';
              //receita1
              strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][3] + '</Data></Cell>';
              //PERIODICIDAD
              strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrSetupDCTF_Purchases_Inv[i][4] + '</Data></Cell>';
              //MONTO
              monto = Number(ArrSetupDCTF_Purchases_Inv[i][5]).toFixed(2);
              strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

              strRetenciones_aux += '</Row>';
              obtenerVectorJournalTributo(ArrSetupDCTF_Purchases_Inv[i][1], ArrSetupDCTF_Purchases_Inv[i][3], ArrSetupDCTF_Purchases_Inv[i][4]);
              obtenerR12_R14Journal(ArrSetupDCTF_Purchases_Inv[i][1], ArrSetupDCTF_Purchases_Inv[i][3], ArrSetupDCTF_Purchases_Inv[i][4], ArrJournalR12, LineasR12);
              obtenerR12_R14Journal(ArrSetupDCTF_Purchases_Inv[i][1], ArrSetupDCTF_Purchases_Inv[i][3], ArrSetupDCTF_Purchases_Inv[i][4], ArrJournalR14, LineasR14);
            }
          }
        }
      }

      var importacionesTotal = ArrJournalImportacion.concat(ArrIOFBillPay, ArrCIDEBillPay);
      agregarLineasImportacion(importacionesTotal);

      agregarSeparador = true;
      for (var j = 0; j < Arr_WHT_No_Item.length; j++) {
        if (Arr_WHT_No_Item[j][0] == '02' || Arr_WHT_No_Item[j][0] == '11') {
          if (agregarSeparador) {
            colocarSeparador("RETENCIONES");
            agregarSeparador = false;
          }
          strRetenciones_aux += '<Row>';
          //NUMERO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + Arr_WHT_No_Item[j][6] + '</Data></Cell>';
          //TIPO DOC
          //Tributo
          if (Arr_WHT_No_Item[j][0] == '02') {
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String"> Bill </Data></Cell>';
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">IRRF</Data></Cell>';
          }
          if (Arr_WHT_No_Item[j][0] == '11') {
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String"> Bill Payment </Data></Cell>';
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">CSRF</Data></Cell>';
          }
          //codigo de TRIBUTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + Arr_WHT_No_Item[j][0] + '</Data></Cell>';
          //receita1
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + Arr_WHT_No_Item[j][3] + '</Data></Cell>';
          //PERIODICIDAD
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + Arr_WHT_No_Item[j][5] + '</Data></Cell>';
          //MONTO
          monto = Number(Arr_WHT_No_Item[j][4]);
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

          strRetenciones_aux += '</Row>';
          obtenerVectorJournalTributo(Arr_WHT_No_Item[j][0], Arr_WHT_No_Item[j][3], Arr_WHT_No_Item[j][5]);
          obtenerR12_R14Journal(Arr_WHT_No_Item[j][0], Arr_WHT_No_Item[j][3], Arr_WHT_No_Item[j][5], ArrJournalR12, LineasR12);
          obtenerR12_R14Journal(Arr_WHT_No_Item[j][0], Arr_WHT_No_Item[j][3], Arr_WHT_No_Item[j][5], ArrJournalR14, LineasR14);
        }
      }

      agregarSeparador = true;
      for (var j = 0; j < Arr_WHT_Item.length; j++) {
        if (Arr_WHT_Item[j][0] == '02') {
          if (agregarSeparador) {
            colocarSeparador("RETENCIÓN REPRESENTANTE LEGAL");
            agregarSeparador = false;
          }
          strRetenciones_aux += '<Row>';
          //NUMERO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + Arr_WHT_Item[j][6] + '</Data></Cell>';
          //TIPO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">Bill</Data></Cell>';
          //Tributo
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">IRRF</Data></Cell>';
          //codigo de TRIBUTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_tribute_wht_irrf_repres + '</Data></Cell>';
          //receita1
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_receita_wht_irrf_repres + '</Data></Cell>';
          //PERIODICIDAD
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + periodicidad_irrf_repres + '</Data></Cell>';
          //MONTOº
          monto = Arr_WHT_Item[j][4];
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

          strRetenciones_aux += '</Row>';
          obtenerVectorJournalTributo(Arr_WHT_Item[j][0], Arr_WHT_Item[j][3], Arr_WHT_Item[j][5]);
          obtenerR12_R14Journal(Arr_WHT_Item[j][0], Arr_WHT_Item[j][3], Arr_WHT_Item[j][5], ArrJournalR12, LineasR12);
          obtenerR12_R14Journal(Arr_WHT_Item[j][0], Arr_WHT_Item[j][3], Arr_WHT_Item[j][5], ArrJournalR14, LineasR14);
        }
      }
      ///CARGAR PAYROLL
      if (Type_concept_journal == '1') {
        if (ArrPayroll.length != 0) {
          colocarSeparador("RETENCIÓN PAYROLL");
          for (var i = 0; i < ArrPayroll.length; i++) {
            strRetenciones_aux += '<Row>';
            //NUMERO DOC
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ' ' + '</Data></Cell>';
            //TIPO DOC
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrPayroll[i][1] + '</Data></Cell>';
            //Tributo
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">IRRF</Data></Cell>';
            //codigo de TRIBUTO
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_tributo_payroll + '</Data></Cell>';
            //receita1
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_receta_payroll + '</Data></Cell>';
            //PERIODICIDAD
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_periodicidad_payroll + '</Data></Cell>';
            //MONTO
            monto = ArrPayroll[i][0];
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

            strRetenciones_aux += '</Row>';
            obtenerVectorJournalTributo(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll);
            obtenerR12_R14Journal(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll, ArrJournalR12, LineasR12);
            obtenerR12_R14Journal(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll, ArrJournalR14, LineasR14);
          }
        }
      } else {
        if (ArrPayrollCPRB.length != 0) {
          colocarSeparador("RETENCIÓN PAYROLL");
          for (var i = 0; i < ArrPayrollCPRB.length; i++) {
            strRetenciones_aux += '<Row>';
            //NUMERO DOC
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ' ' + '</Data></Cell>';
            //TIPO DOC
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrPayrollCPRB[i][1] + '</Data></Cell>';
            //Tributo
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">CPSS</Data></Cell>';
            //codigo de TRIBUTO
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_tributo_payroll + '</Data></Cell>';
            //receita1
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_receta_payroll + '</Data></Cell>';
            //PERIODICIDAD
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + id_periodicidad_payroll + '</Data></Cell>';
            //MONTO
            monto = ArrPayrollCPRB[i][0];
            strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

            strRetenciones_aux += '</Row>';
            obtenerVectorJournalTributo(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll);
            obtenerR12_R14Journal(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll, ArrJournalR12, LineasR12);
            obtenerR12_R14Journal(id_tributo_payroll, id_receta_payroll, id_periodicidad_payroll, ArrJournalR14, LineasR14);
          }
        }
      }
      /************************************** CARGAR IMPOSTOS DE NOMINA *********************************************/
      if (ArrLineasNomina.length != 0) {
        colocarSeparador("IMPUESTOS SOBRE LA NÓMINA");
        for (var i = 0; i < ArrLineasNomina.length; i++) {
          strRetenciones_aux += '<Row>';
          //NUMERO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][0] + '</Data></Cell>';
          //TIPO DOC
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][1] + '</Data></Cell>';
          //Tributo
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][4] + '</Data></Cell>';
          //codigo de TRIBUTO
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][5] + '</Data></Cell>';
          //receita1
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][6] + '</Data></Cell>';
          //PERIODICIDAD
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + ArrLineasNomina[i][7] + '</Data></Cell>';
          //MONTO
          monto = ArrLineasNomina[i][11];
          strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

          strRetenciones_aux += '</Row>';
        }
      }

      strRetenciones_aux += '</Table></Worksheet>';
      var arrPagos = JournalsCoinciR11.concat(ArrLineasNomina);
      strRetenciones_aux += GenerarHojaPagosJournal(JournalsCoinciR11);
      log.debug('LineasR12', LineasR12);
      strRetenciones_aux += GenerarHojaCompensacionesJournal(LineasR12);
      log.debug('LineasR14', LineasR14);
      strRetenciones_aux += GenerarHojaSuspensionesJournal(LineasR14);
      strRetenciones_aux += '</Workbook>';

      strRetenciones = encode.convert({
        string: strRetenciones_aux,
        inputEncoding: encode.Encoding.UTF_8,
        outputEncoding: encode.Encoding.BASE_64
      });
    }

    function agregarLineasImportacion(arrayJournal) {

      for (var i = 0; i < arrayJournal.length; i++) {
        if (i == 0) {
          colocarSeparador("IMPORTACIONES");
        }
        strRetenciones_aux += '<Row>';
        //NUMERO DOC
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + arrayJournal[i][9] + '</Data></Cell>';
        //TIPO DOC
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + arrayJournal[i][0] + '</Data></Cell>';
        //Tributo
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + arrayJournal[i][2] + '</Data></Cell>';
        //codigo de TRIBUTO
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + arrayJournal[i][1] + '</Data></Cell>';
        //receita
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + arrayJournal[i][3] + '</Data></Cell>';
        //PERIODICIDAD
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="String">' + arrayJournal[i][4] + '</Data></Cell>';
        //MONTO
        monto = Number(arrayJournal[i][5]).toFixed(2);
        strRetenciones_aux += '<Cell ss:StyleID="s25"><Data ss:Type="Number">' + monto + '</Data></Cell>';

        strRetenciones_aux += '</Row>';

        obtenerVectorJournalTributo(arrayJournal[i][1], arrayJournal[i][3], arrayJournal[i][4]);
        obtenerR12_R14Journal(arrayJournal[i][1], arrayJournal[i][3], arrayJournal[i][4], ArrJournalR12, LineasR12);
        obtenerR12_R14Journal(arrayJournal[i][1], arrayJournal[i][3], arrayJournal[i][4], ArrJournalR14, LineasR14);
      }
    }

    function validarCaracteres(s) {
      var AccChars = "./-%";
      var RegChars = "";

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

    function ObtenerDatosSubsidiaria() {
      var configpage = config.load({
        type: config.Type.COMPANY_INFORMATION
      });

      if (feature_Subsi) {
        companyname = ObtainNameSubsidiaria(param_Subsi);
        companyruc = ObtainFederalIdSubsidiaria(param_Subsi);
      } else {
        companyruc = configpage.getValue('employerid');
        companyname = configpage.getValue('legalname');
      }

      companyruc = companyruc.replace(' ', '');
      companyruc = validarCaracteres(companyruc);
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
        // libreria.sendMail(LMRY_script, ' [ ObtainNameSubsidiaria ] ' + err);
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
        // libreria.sendMail(LMRY_script, ' [ ObtainFederalIdSubsidiaria ] ' + err);
      }
      return '';
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
      param_RecordLogID = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_logid'
      });
      param_Periodo = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_periodo'
      });
      param_Subsi = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_subsid'
      });
      param_Multi = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_multibook'
      });
      param_Type_Decla = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_tipo_decla'
      });
      param_Num_Recti = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_num_rec'
      });
      param_Lucro_Conta = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_lucro'
      });
      param_Monto_Adicional = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_monto_adic'
      });
      param_Monto_Excluyente = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_monto_excl'
      });
      param_Feature = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_featureid'
      });
      param_File = objContext.getParameter({
        name: 'custscript_lmry_br_dctf_xls_fileid'
      });

      log.debug({
        title: 'Parametros',
        details: param_RecordLogID + '-' + param_Periodo + '-' + param_Subsi + '-' + param_Multi + '-' + param_Type_Decla + '-' + param_Num_Recti + '-' + param_Lucro_Conta + '-' + param_Monto_Adicional + '-' + param_Monto_Excluyente + '-' + param_Feature + '-' + param_File
      });

      if (param_Feature != null && param_Feature != '') {
        NameReport = search.lookupFields({
          type: 'customrecord_lmry_br_features',
          id: param_Feature,
          columns: ['name']
        });
        NameReport = NameReport.name;
      }

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
      /*******************************  PERIODO  ***************************************/
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

        if (result.length != 0) {
          for (var i = 0; i < result.length; i++) {
            periodenddate_temp = {
              periodname: result[i].getValue('name'),
              enddate: result[i].getValue('custrecord_lmry_date_fin')
            };
          }
        } else {
          log.debug('Alerta', 'No se configuró periodo en Special Accounting Period');
          periodenddate_temp = search.lookupFields({
            type: search.Type.ACCOUNTING_PERIOD,
            id: param_Periodo,
            columns: ['enddate', 'periodname']
          });
        }

      } else {
        var periodenddate_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_PERIOD,
          id: param_Periodo,
          columns: ['enddate', 'periodname']
        });
      }

      log.debug('periodenddate_temp', periodenddate_temp);
      if (periodenddate_temp != null) {
        //Period EndDate
        var periodenddate = periodenddate_temp.enddate;
        //Nuevo Formato Fecha
        var parsedDateStringAsRawDateObject = format.parse({
          value: periodenddate,
          type: format.Type.DATE
        });

        var MM = parsedDateStringAsRawDateObject.getMonth() + 1;
        var AAAA = parsedDateStringAsRawDateObject.getFullYear();
        var DD = parsedDateStringAsRawDateObject.getDate();

        if ((DD + '').length == 1) {
          DD = '0' + DD;
        }
        if ((MM + '').length == 1) {
          MM = '0' + MM;
        }

        mes_date = MM;
        anio_date = AAAA + '';
        //Period Name
        periodname = periodenddate_temp.periodname;
      } else {
        log.debug('Alerta Periodo', 'No se tiene configurado el periodo contable');
      }
      /***************************** DATOS DE SUBSIDIARIA *********************************/
      var subsi_id = 0;
      if (feature_Subsi) {
        subsi_id = param_Subsi;
      } else {
        subsi_id = 1;
      }

      var subsi_temp = search.lookupFields({
        type: search.Type.SUBSIDIARY,
        id: subsi_id,
        columns: ['custrecord_lmry_br_porc_economic_activiy', 'custrecord_lmry_br_regimen_pis_confis']
      });

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

      if (feature_Multi) {
        //Multibook Name
        var multibookName_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_BOOK,
          id: param_Multi,
          columns: ['name']
        });
        multibookName = multibookName_temp.name;
      }
    }

    return {
      execute: execute
    };
  });
