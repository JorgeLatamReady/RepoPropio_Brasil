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
define(["N/url", 'N/search', 'N/log', 'require', 'N/file', "N/config", 'N/runtime', 'N/query', "N/format", "N/record", "N/task", './BR_LIBRERIA_MENSUAL/LMRY_BR_Reportes_LBRY_V2.0.js', "SuiteBundles/Bundle 37714/LatamReady/LMRY_EI_MD5"],

  function(url, search, log, require, fileModulo, config, runtime, query, format, recordModulo, task, libreria, md5) {

    var objContext = runtime.getCurrentScript();
    var LMRY_script = 'LMRY_BR_e-NotaFiscal_NFCE _MPRD_V2.0.js';

    // Parámetros
    var paramPeriod = objContext.getParameter({
      name: 'custscript_lmry_nfce_periodo'
    });
    var paramSubsi = objContext.getParameter({
      name: 'custscript_lmry_nfce_subsidiary'
    });
    var paramMulti = objContext.getParameter({
      name: 'custscript_lmry_nfce_multibook'
    });
    var paramFeature = objContext.getParameter({
      name: 'custscript_lmry_nfce_feature'
    });
    var paramLogID = objContext.getParameter({
      name: 'custscript_lmry_nfce_logid'
    });
    var paramModeloNF = objContext.getParameter({
      name: 'custscript_lmry_nfce_modelo'
    });

    var language = runtime.getCurrentScript().getParameter({
      name: 'LANGUAGE'
    }).substring(0, 2);

    // Features
    var featureSubs = runtime.isFeatureInEffect({
      feature: 'SUBSIDIARIES'
    });
    var featureMulti = runtime.isFeatureInEffect({
      feature: 'MULTIBOOK'
    });

    // Variables
    var periodName = '';
    var multibookName = '';
    var companyname = '';
    var companyCNPJ = '';
    var mes_date = '';
    var anio_date = '';
    var uf_domicilio = '';
    var nameReport = '';

    function getInputData() {
      try {
        log.debug('Inicia reporte con modelo de nf: ', paramModeloNF);
        var arrNoCancelNetsuite = [];
        arrNoCancelNetsuite = obtenerLineasItem(false);
        log.debug('data voided false', arrNoCancelNetsuite);
        var arrCancelNetsuite = [];
        arrCancelNetsuite = obtenerLineasItem(true);
        log.debug('data voided true', arrCancelNetsuite);

        var arrReturn = arrNoCancelNetsuite.concat(arrCancelNetsuite);
        if (arrReturn.length > 0) {
          log.debug('data inicial', arrReturn);
          return arrReturn;
        } else {
          log.debug('no hay data');
        }
      } catch (error) {
        log.error('Error de getInputData', error);
        return [{
          "isError": "T",
          "error": error
        }];
      }
    }

    function map(context) {
      try {
        var arrMap = JSON.parse(context.value);

        if (arrMap["isError"] == "T") {
          context.write({
            key: context.key,
            value: arrMap
          });
        } else {
          var key = arrMap[1]; //id transaction
          var strLineasItem = '';

          var taxResults = obtenerTaxResults(arrMap[1], arrMap[0]);

          var baseCalculo = 0;
          var impICMS = 0;
          var isentas = 0;
          var outras = 0;
          var alicuotaICMS = 0;
          var alicuotaPIS = 0;
          var impPIS = 0;
          var alicuotaCOFINS = 0;
          var impCOFINS = 0;

          for (var i = 0; i < taxResults.length; i++) {

            if (taxResults[i][3] == 'ICMS') {
              if (taxResults[i][2] == 0) {
                var cst = arrMap[10];

                if (cst == '090') { //OUTRAS
                  outras = Number(outras) + Number(taxResults[i][0]);
                }

                if (cst == '040' || cst == '041') { //ISENTAS / NAO TRIBUT.
                  isentas = Number(isentas) + Number(taxResults[i][0]);
                }

              } else {
                var cst = taxResults[i][4];
                if (cst != '090' && cst != '040' && cst != '041' && cst != '' && cst != null) {
                  baseCalculo = Number(baseCalculo) + Number(taxResults[i][0]);
                  impICMS = Number(impICMS) + Number(taxResults[i][1]);
                  alicuotaICMS = taxResults[i][2];
                }
              }

            }

            if (taxResults[i][3] == 'PIS') {
              if (taxResults[i][2] != 0) {
                impPIS = Number(impPIS) + Number(taxResults[i][1]);
                alicuotaPIS = taxResults[i][2];
              }
            }

            if (taxResults[i][3] == 'COFINS') {
              if (taxResults[i][2] != 0) {
                impCOFINS = Number(impCOFINS) + Number(taxResults[i][1]);
                alicuotaCOFINS = taxResults[i][2];
              }
            }

          }

          if (impICMS != 0 || isentas != 0 || outras != 0) {
            var arrTempLinea = armarLineasItem(arrMap, baseCalculo, impICMS, alicuotaICMS, isentas, outras, impPIS, alicuotaPIS, impCOFINS, alicuotaCOFINS);
            if (arrTempLinea != null) {
              var idContract = '';
              if (arrMap[6] == '06') {
                arrMap[4] ? idContract = arrMap[4] : idContract = '';
              } else {
                arrMap[18] ? idContract = arrMap[18] : idContract = '';
              }
              context.write({
                key: key,
                value: {
                  lineasItem: arrTempLinea,
                  customer: arrMap[2],
                  transaction: arrMap[1],
                  idContractDetail: idContract,
                  valorTotalFactura: arrMap[22]
                }
              });
            }
          } else {
            log.debug('linea de item no tiene icms');
          }

        }
      } catch (err) {
        log.error('Error de Map', err);
        context.write({
          key: arrTemp[1], //id transaction
          value: {
            isError: "T",
            error: err
          }
        });
      }
    }

    function reduce(context) {
      try {
        var vectorReduce = context.values;
        var lineasTransaction = [];
        var transactionID = '';
        var customerID = '';
        var contractID = '';
        var strArchivoItem = '';
        var strArchivoMaestre = '';
        var strArchivoDadosCadastrais = '';

        var uf = '';
        var claseConsumo = '';
        var tipoUtilizacao = '';
        var grupoTension = '';
        var dataEmisao = '';
        var modelo = '';
        var serie = '';
        var numero = '';
        var situacaoDocumento = '';
        var anoMesApuracao = '';
        var numTerminal = '';
        var lecturaActual = '';
        var lecturaAnterior = '';

        var valorTotal = 0.00;
        var bcICMS = 0.00;
        var icmsDestacado = 0.00;
        var isentasNoTrib = 0.00;
        var outrosValores = 0.00;
        var impPIS = 0.00;
        var impCOFINS = 0.00;
        var valorTotalFactura;

        obtenerDatosSubsidiaria();
        //log.debug('data recibida reduce', vectorReduce);
        for (var j = 0; j < vectorReduce.length; j++) {
          var obj = JSON.parse(vectorReduce[j]);
          if (obj["isError"] == "T") {
            log.debug('se envia error a summarize');
            context.write({
              key: context.key,
              value: obj
            });
            return;
          }
          lineasTransaction.push(obj["lineasItem"]);

          if (transactionID == '') {
            transactionID = obj["transaction"];
            customerID = obj["customer"];
            contractID = obj["idContractDetail"];
            valorTotalFactura = obj["valorTotalFactura"];

            /**********************  REUSO DE CAMPOS REPETIDOS PARA ARCHIVO MAESTRE *******************/
            uf = lineasTransaction[j][1];
            claseConsumo = lineasTransaction[j][2];
            tipoUtilizacao = lineasTransaction[j][3];
            grupoTension = lineasTransaction[j][4];
            dataEmisao = lineasTransaction[j][5];
            modelo = lineasTransaction[j][6];
            serie = lineasTransaction[j][7];
            numero = lineasTransaction[j][8];
            situacaoDocumento = lineasTransaction[j][25];
            anoMesApuracao = lineasTransaction[j][26];
            numTerminal = lineasTransaction[j][27];
            lecturaActual = lineasTransaction[j][39];
            lecturaAnterior = lineasTransaction[j][38];
            numeroFComercial = lineasTransaction[j][40];
          }

          /********************  MONTOS PARA ARCHIVO MAESTRE ***********************+*/
          valorTotal += Number(lineasTransaction[j][17]);
          bcICMS += Number(lineasTransaction[j][20]);
          icmsDestacado += Number(lineasTransaction[j][21]);
          isentasNoTrib += Number(lineasTransaction[j][22]);
          outrosValores += Number(lineasTransaction[j][23]);
          /***************************  ARMADO ARCHIVO ITEM *************************+*/
          strArchivoItem += formatearLineasItem(lineasTransaction[j], j);
          if (j != vectorReduce.length - 1) {
            strArchivoItem += '|';
          }
        }
        /******************** BUSQUEDA EN RECORDS ***********************/
        var cliente = search.lookupFields({
          type: search.Type.CUSTOMER,
          id: customerID,
          columns: ['vatregnumber', 'custentity_lmry_br_state_tax_subscr', 'isperson', 'companyname', 'firstname',
            'lastname', 'custentity_lmry_br_client_type', 'address.zipcode', 'address.address1', 'address.address2',
            'address.custrecord_lmry_addr_city', 'address.custrecord_lmry_addr_prov_acronym', 'address.addressphone',
            'address.custrecord_lmry_address_number', 'address.custrecord_lmry_addr_reference',
            'address.custrecord_lmry_addr_city_id', 'custentity_lmry_countrycode'
          ]
        });

        var cityArrayJson = cliente['address.custrecord_lmry_addr_city'];

        if (cityArrayJson != null && cityArrayJson != '') {
          var city = search.lookupFields({
            type: 'customrecord_lmry_city',
            id: cityArrayJson[0].value,
            columns: ['custrecord_lmry_city_ddd']
          });
        }

        var idTipoCliente = cliente.custentity_lmry_br_client_type;
        if (idTipoCliente != null && idTipoCliente != '') {
          var clienteType = search.lookupFields({
            type: 'customrecord_lmry_br_client_type',
            id: idTipoCliente[0].value,
            columns: ['custrecord_lmry_br_code_client']
          });
        }

        var brContractDetail = search.lookupFields({
          type: 'customrecord_lmry_br_contract_detail',
          id: contractID,
          columns: ['custrecord_lmry_br_subclass_energy', 'custrecord_lmry_br_principal_terminal']
        });

        /********************** ARMAR LINEA ARCHIVO MAESTRE ***********************/
        var lineaTempMaestre = [];
        var cadenaMD5_13 = '';

        //0. CNPJ O CPF
        var cnpjCPF = '';
        if (cliente.custentity_lmry_countrycode == '1058') {
          cnpjCPF = validaNumeros(cliente.vatregnumber);
        }
        lineaTempMaestre.push(completar(14, cnpjCPF, true, '0'));
        //1. IE
        var ie = validaNumeros(cliente.custentity_lmry_br_state_tax_subscr);
        if (ie == null || ie == '' || cliente.custentity_lmry_countrycode != '1058') {
          ie = 'ISENTO'
        }
        lineaTempMaestre.push(completar(14, ie, false, ' '));
        //2. RAZON SOCIAL
        if (cliente.isperson) {
          var razonSocial = cliente.firstname + '' + cliente.lastname;
        } else {
          var razonSocial = cliente.companyname;
        }
        lineaTempMaestre.push(completar(35, razonSocial, false, ' '));
        //3. UF
        lineaTempMaestre.push(uf);
        //4. Classe de Consumo ou Tipo de Assinante
        lineaTempMaestre.push(claseConsumo);
        //5. Fase ou Tipo de Utilizaçao
        lineaTempMaestre.push(tipoUtilizacao);
        //6. Grupo de Temsao
        lineaTempMaestre.push(completar(2, grupoTension, true, '0'));
        //7. Codigo de Identificação do consumidor ou assinante
        lineaTempMaestre.push(completar(12, customerID, false, ' '));
        //8. Data de emissao
        lineaTempMaestre.push(dataEmisao);
        //9. Modelo
        lineaTempMaestre.push(modelo);
        //10. Serie
        lineaTempMaestre.push(completar(3, serie, true, '0'));
        //11. Numero
        lineaTempMaestre.push(completar(9, numero, true, '0'));
        //12. Codigo de Autenticaçao Digital documento fisca
        lineaTempMaestre.push('');
        //13. Valor Total
        lineaTempMaestre.push(completar(12, validaNumeros((redondear(valorTotal)).toFixed(2)), true, '0'));
        //14. BC ICMS
        lineaTempMaestre.push(completar(12, validaNumeros((redondear(bcICMS)).toFixed(2)), true, '0'));
        //15. ICMS Destacado
        lineaTempMaestre.push(completar(12, validaNumeros((redondear(icmsDestacado)).toFixed(2)), true, '0'));
        //16. Operaçoes Isentas or nao tributadas
        lineaTempMaestre.push(completar(12, validaNumeros((redondear(isentasNoTrib)).toFixed(2)), true, '0'));
        //17. Outros valores
        lineaTempMaestre.push(completar(12, validaNumeros((redondear(outrosValores)).toFixed(2)), true, '0'));
        //18. Situaçao do documento
        lineaTempMaestre.push(situacaoDocumento);
        //19. Ano e Mes de referencia de apuraçao
        lineaTempMaestre.push(anoMesApuracao);
        //20. Referencia (Se llena en summarize)
        lineaTempMaestre.push('');
        //21. Numero Terminal Telefonico ou Numero da conta de consumo (chquear !!!)
        lineaTempMaestre.push(completar(12, numTerminal, false, ' '));
        //22. Indicação do tipo de informação contida no campo 1
        var indicadorTipo = '';
        if (cliente.custentity_lmry_countrycode != '1058') { //extranjero
          cliente.isperson ? indicadorTipo = '4' : indicadorTipo = '3'
        } else {
          cliente.isperson ? indicadorTipo = '2' : indicadorTipo = '1'
        }
        lineaTempMaestre.push(indicadorTipo);
        //23. Tipo de cliente
        var tipoCliente = clienteType.custrecord_lmry_br_code_client;
        lineaTempMaestre.push(tipoCliente);
        //24. Subclasse de consumo
        if (modelo == '06') {
          var idSubclass = brContractDetail.custrecord_lmry_br_subclass_energy[0].value

          var subclassConsumo = search.lookupFields({
            type: 'customrecord_lmry_consumtio_subclass',
            id: idSubclass,
            columns: ['custrecord_lmry_br_code_subclass']
          });

          lineaTempMaestre.push(subclassConsumo.custrecord_lmry_br_code_subclass);
        } else {
          lineaTempMaestre.push('00');
        }
        //25. Número do terminal telefônico principal
        if (modelo == '06') {
          lineaTempMaestre.push('            ');
        } else {
          lineaTempMaestre.push(completar(12, brContractDetail.custrecord_lmry_br_principal_terminal, false, ' '));
        }
        //26. CNPJ do emitente
        lineaTempMaestre.push(companyCNPJ);
        //27. Número ou código da fatura comercial
        var numFactComercial = '';
        if (numeroFComercial != '' && numeroFComercial != null) {
          numFactComercial = numeroFComercial;
        }
        lineaTempMaestre.push(completar(20, numFactComercial, true, '0'));
        //28. Valor total da fatura comercial
        lineaTempMaestre.push(completar(12, validaNumeros(redondear(valorTotalFactura).toFixed(2)), true, '0'));
        //29. Data de leitura anterior
        if (modelo == '06') {
          lineaTempMaestre.push(lecturaAnterior);
        } else {
          lineaTempMaestre.push('00000000');
        }
        //30. Data de leitura atual
        if (modelo == '06') {
          lineaTempMaestre.push(lecturaActual);
        } else {
          lineaTempMaestre.push('00000000');
        }
        //31. Brancos - reservado para uso futuro
        lineaTempMaestre.push(completar(50, '', true, ' '));
        //32. Brancos - reservado para uso futuro
        lineaTempMaestre.push(completar(8, '', true, '0'));
        //33. Informações adicionais
        lineaTempMaestre.push(completar(30, '', true, ' '));
        //34. Brancos - reservado para uso futuro
        lineaTempMaestre.push('     ');

        /* ARMADO TOKEN MD5 Campo 13 */
        cadenaMD5_13 = lineaTempMaestre[0] + lineaTempMaestre[11] + lineaTempMaestre[13] + lineaTempMaestre[14] + lineaTempMaestre[15] + lineaTempMaestre[8] + lineaTempMaestre[26];
        var token = md5.md5_general(cadenaMD5_13);
        lineaTempMaestre[12] = token;
        /*****************************/
        //log.debug('linea temp maestre', lineaTempMaestre);
        /********************** ARMAR LINEA ARCHIVO DADOS CADASTRAIS ***********************/
        var lineaTempDadosCastrais = [];
        //0. CNPJ O CPF
        lineaTempDadosCastrais.push(lineaTempMaestre[0]);
        //1. IE
        lineaTempDadosCastrais.push(lineaTempMaestre[1]);
        //2. RAZON SOCIAL
        lineaTempDadosCastrais.push(lineaTempMaestre[2]);
        //3. Logradouro
        var logradouro = cliente['address.address1'];
        lineaTempDadosCastrais.push(completar(45, logradouro, false, ' '));
        //4. Numero
        var numeroAddress = cliente['address.custrecord_lmry_address_number'];
        lineaTempDadosCastrais.push(completar(5, numeroAddress, true, '0'));
        //5. Complemento
        var complemento = cliente['address.custrecord_lmry_addr_reference'];
        lineaTempDadosCastrais.push(completar(15, complemento, false, ' '));
        //6. CEP
        var zip = cliente['address.zipcode'];
        lineaTempDadosCastrais.push(completar(8, validaNumeros(zip), true, '0'));
        //7. Bairro
        var address2 = cliente['address.address2'];
        lineaTempDadosCastrais.push(completar(15, address2, false, ' '));
        //8. Município
        var municipio = '';
        if (cliente.custentity_lmry_countrycode == '1058') {
          if (cityArrayJson != null && cityArrayJson != '') {
            municipio = cityArrayJson[0].text;
          } else {
            log.debug('Alerta', 'No se configuro municipio en archivo Dados Cadastrais');
          }
          lineaTempDadosCastrais.push(completar(30, municipio, false, ' '));
        } else {
          lineaTempDadosCastrais.push(completar(30, municipio, false, ' '));
        }
        //9. UF (Cliente)
        var uf = cliente['address.custrecord_lmry_addr_prov_acronym'];
        if (cliente.custentity_lmry_countrycode != '1058') {
          uf = 'EX'
        }
        lineaTempDadosCastrais.push(uf);
        //10. Telefone de contato
        var phone = cliente['address.phone'];
        lineaTempDadosCastrais.push(completar(12, phone, false, ' '));
        //11. Código de identificação do consumidor ou assinante
        lineaTempDadosCastrais.push(completar(12, customerID, false, ' '));
        //12. Número do terminal telefônico ou da unidade consumidora
        var numTerminal = '';
        numTerminal = lineaTempMaestre[21];
        lineaTempDadosCastrais.push(completar(12, numTerminal, false, ' '));
        //13. UF de habilitação do terminalMtelefônico
        lineaTempDadosCastrais.push(uf);
        //14. Data de emissão
        lineaTempDadosCastrais.push(dataEmisao);
        //15. Modelo
        lineaTempDadosCastrais.push(modelo);
        //16. Serie
        lineaTempDadosCastrais.push(completar(3, serie, true, '0'));
        //17. Numero
        lineaTempDadosCastrais.push(completar(9, numero, true, '0'));
        //18. Código do Município
        var cod_muni = '';
        if (cliente.custentity_lmry_countrycode == '1058') {
          cod_muni = cliente['address.custrecord_lmry_addr_city_id'];
        }
        lineaTempDadosCastrais.push(completar(7, cod_muni, true, '0'));

        //19. Brancos - reservado para uso futuro
        lineaTempDadosCastrais.push('     ');
        //20. Código de Autenticação Digital do registro
        var contentMD5 = lineaTempDadosCastrais.join('');
        lineaTempDadosCastrais.push(md5.md5_general(contentMD5));

        //log.debug('lineaTempDadosCastrais', lineaTempDadosCastrais);

        context.write({
          key: context.key,
          value: {
            maestre: lineaTempMaestre,
            item: strArchivoItem,
            dadosCadastrais: lineaTempDadosCastrais
          }
        });
      } catch (e) {
        log.error('Error de Reduce', e);
        context.write({
          key: context.key,
          value: {
            isError: "T",
            error: e
          }
        });
      }
    }

    function summarize(context) {
      try {
        var strItem = '';
        var strMaestre = '';
        var strDadosCadastrais = '';
        var referenciaItem = 1;
        var errores = [];
        var FILE_MAX_SIZE = 6000000;
        var tamanoArchivo = 0;
        var arrayGeneral = [];
        /* variables para nombre de archivo */
        var modelo = '';
        var serie = '';
        var name_file = '';
        var secuenciaArchivo = 1;
        var flag = true;
        var id_carpeta;
        var name_carpeta = '';

        obtenerDatosSubsidiaria();

        context.output.iterator().each(function(key, value) {
          var obj = JSON.parse(value);
          if (obj["isError"] == "T") {
            log.debug('hay error');
            errores.push(JSON.stringify(obj["error"]));
            return false;
          } else {
            log.debug('hay data buena');
            arrayGeneral.push(obj);
            return true;
          }
        });

        if (errores.length > 0) {
          libreria.sendemailTranslate(errores[0], LMRY_script, language);
          actualizarLog('', 'error');
          return;
        }

        log.debug('arrayGeneral', arrayGeneral);
        /********** ORDENAMIENTO NRO DOCUMENTO SEQUENCIAL ASCENDENTE ***************/
        for (var j = 0; j < arrayGeneral.length - 1; j++) {
          for (var i = 0; i < arrayGeneral.length - 1; i++) {

            if (Number(arrayGeneral[i].maestre[11]) > Number(arrayGeneral[i + 1].maestre[11])) {
              var tmp = arrayGeneral[i + 1];
              arrayGeneral[i + 1] = arrayGeneral[i];
              arrayGeneral[i] = tmp;
            }

          }
        }
        log.debug('arrayGeneral ordenado', arrayGeneral);
        if (arrayGeneral.length > 0) {
          if (featureMulti) {
            name_carpeta = 'nfce_' + companyCNPJ + '_' + paramMulti + '_' + paramSubsi + '_' + anio_date + mes_date;
          } else {
            name_carpeta = 'nfce_' + companyCNPJ + '_' + paramSubsi + '_' + anio_date + mes_date;
          }
          id_carpeta = obtenerFichero(name_carpeta);
          log.debug('id_carpeta', id_carpeta);
        } else {
          actualizarLog('', 'nodata');
        }

        for (var i = 0; i < arrayGeneral.length; i++) {
          /*************** ARMADO ARCHIVO ITEM ***********************/
          var arrayItemTemp = (arrayGeneral[i].item).split('|');
          if (referenciaItem != 1) {
            strItem += '\r\n';
          }
          strItem += arrayItemTemp.join('\r\n');
          /*************** ARMADO ARCHIVO MAESTRE ***********************/
          arrayMaestre = arrayGeneral[i].maestre;
          /* variables para nombre de archivo */
          serie = arrayMaestre[10];
          /* ******************************* */
          arrayMaestre[20] = completar(9, referenciaItem, true, '0');
          var tokenFinal = md5.md5_general(arrayMaestre.join(''));
          arrayMaestre.push(tokenFinal);
          if (referenciaItem != 1) {
            strMaestre += '\r\n';
          }
          strMaestre += arrayMaestre.join('')
          /*************** ARMADO ARCHIVO DADOS CADASTRAIS ***********************/
          var arrayDadosCadastrais = arrayGeneral[i].dadosCadastrais;
          if (referenciaItem != 1) {
            strDadosCadastrais += '\r\n';
          }
          strDadosCadastrais += arrayDadosCadastrais.join('');
          //log.debug('strMaestre', strMaestre);
          //log.debug('strItem', strItem);
          //log.debug('strDadosCadastrais', strDadosCadastrais);
          /*************************** GUARDADO DE ARCHIVOS ***********************/
          name_file = uf_domicilio + companyCNPJ + paramModeloNF + serie + completar(2, anio_date, true, '') + mes_date + 'N01';
          tamanoArchivo = lengthInUtf8Bytes(strItem);
          //log.debug('tamano archivo item', tamanoArchivo);
          if (tamanoArchivo > FILE_MAX_SIZE) {
            if (id_carpeta != '') {
              saveFile(id_carpeta, strItem, name_file, 'item', secuenciaArchivo);
              saveFile(id_carpeta, strMaestre, name_file, 'maestre', secuenciaArchivo);
              saveFile(id_carpeta, strDadosCadastrais, name_file, 'dadoscadastrais', secuenciaArchivo);
              secuenciaArchivo++;
              referenciaItem = 1;
              strItem = '';
              strMaestre = '';
              strDadosCadastrais = '';
              name_file = '';
            } else {
              log.debug('Creacion de Carpeta:', 'No existe el folder de archivos NFCE');
            }

          } else {
            referenciaItem += arrayItemTemp.length;
          }
        }

        if (strItem != '') {
          if (id_carpeta != '') {
            saveFile(id_carpeta, strItem, name_file, 'item', secuenciaArchivo);
            saveFile(id_carpeta, strMaestre, name_file, 'maestre', secuenciaArchivo);
            saveFile(id_carpeta, strDadosCadastrais, name_file, 'dadoscadastrais', secuenciaArchivo);
            actualizarLog(id_carpeta, name_carpeta);
          } else {
            log.debug('Creacion de Carpeta:', 'No existe el folder de archivos NFCE');
          }
        } else {
          if (secuenciaArchivo > 1) {
            actualizarLog(id_carpeta, name_carpeta);
          }
        }

      } catch (err) {
        log.error('error summarize', err);
        libreria.sendemailTranslate(err, LMRY_script, language);
        actualizarLog('', 'error');
      }
    }

    function armarLineasItem(arrMap, baseCalculo, impICMS, alicuotaICMS, isentas, outras, impPIS, alicuotaPIS, impCOFINS, alicuotaCOFINS) {
      var valorTotal = redondear(baseCalculo * arrMap[12]) + redondear(isentas * arrMap[12]) + redondear(outras * arrMap[12]);
      valorTotal = (valorTotal).toFixed(2);
      baseCalculo = (redondear(baseCalculo * arrMap[12])).toFixed(2);
      impICMS = (redondear(impICMS * arrMap[12])).toFixed(2);
      isentas = (redondear(isentas * arrMap[12])).toFixed(2);
      outras = (redondear(outras * arrMap[12])).toFixed(2);
      impPIS = (redondear(impPIS * arrMap[12])).toFixed(2);
      impCOFINS = (redondear(impCOFINS * arrMap[12])).toFixed(2);
      /*
      log.debug('baseCalculo', baseCalculo);
      log.debug('impuesto ICMS', impICMS);
      log.debug('isentas', isentas);
      log.debug('outras', outras);
      log.debug('impuesto PIS', impPIS);
      log.debug('impuesto COFINS', impCOFINS);
      log.debug('ali ICMS', alicuotaICMS);
      log.debug('ali PIS', alicuotaPIS);
      log.debug('ali COFINS', alicuotaCOFINS);
      */
      var idContract = '';
      if (arrMap[6] == '06') {
        arrMap[4] ? idContract = arrMap[4] : idContract = '';
      } else {
        arrMap[18] ? idContract = arrMap[18] : idContract = '';
      }

      var cliente = search.lookupFields({
        type: search.Type.CUSTOMER,
        id: arrMap[2],
        columns: ['vatregnumber', 'custentity_lmry_countrycode']
      });

      var brContractDetail = search.lookupFields({
        type: 'customrecord_lmry_br_contract_detail',
        id: idContract,
        columns: ['name', 'custrecord_lmry_br_class_energy', 'custrecord_lmry_br_connection_type', 'custrecord_lmry_br_use_type', 'custrecord_lmry_br_grp_tension']
      });

      var item = search.lookupFields({
        type: search.Type.ITEM,
        id: arrMap[15],
        columns: ['name', 'custitem_lmry_unit_of_measure_symbol', 'custitem_lmry_br_service_catalog']
      });

      if (item.custitem_lmry_br_service_catalog[0].value == arrMap[13]) {
        var serviceCatalog = search.lookupFields({
          type: 'customrecord_lmry_br_catalog_item',
          id: arrMap[13],
          columns: ['name', 'custrecord_lmry_br_catalog_code']
        });
        //log.debug('serviceCatalog', serviceCatalog);
      } else {
        log.debug('Alerta armado de lineas item en map', 'El service catalog del item no coincide con el de la columna en la transaccion.');
        return null;
      }

      /***********************  SE ARMA LINEAS DE ARCHIVO ITEM ***********************/
      var arrTempLineas = [];

      //0. CNPJ o CPF
      var cnpjCPF = '';
      if (cliente['custentity_lmry_countrycode'] == '1058') {
        cnpjCPF = cliente['vatregnumber'];
      }
      arrTempLineas.push(cnpjCPF);
      //1. UF
      arrTempLineas.push(arrMap[3]);
      //2. Clase de Consumo
      if (arrMap[6] == '06') {
        arrTempLineas.push(brContractDetail.custrecord_lmry_br_class_energy[0].value)
      } else {
        arrTempLineas.push('0');
      }
      //3. Fase ou Tipo de Utilização
      if (arrMap[6] == '06') {
        arrTempLineas.push(brContractDetail.custrecord_lmry_br_connection_type[0].value)
      } else {
        arrTempLineas.push(brContractDetail.custrecord_lmry_br_use_type[0].value)
      }
      //4. Grupo de Tensão
      if (arrMap[6] == '06') {
        arrTempLineas.push(brContractDetail.custrecord_lmry_br_grp_tension[0].value)
      } else {
        arrTempLineas.push('00')
      }
      //5. Data de Emissão
      arrTempLineas.push(arrMap[5]);
      //6. Modelo
      arrTempLineas.push(arrMap[6]);
      //7. Série
      arrTempLineas.push(arrMap[7]);
      //8. Número
      arrTempLineas.push(arrMap[8]);
      //9. CFOP
      arrTempLineas.push(arrMap[14]);
      //10. Nº de ordem do Item
      arrTempLineas.push('');
      //11. Código do item
      arrTempLineas.push(arrMap[15]);
      //12. Descrição do item
      arrTempLineas.push(item.name);
      //13. Código de classificação do item
      arrTempLineas.push(serviceCatalog.custrecord_lmry_br_catalog_code);
      //14. Unidade
      arrTempLineas.push(item.custitem_lmry_unit_of_measure_symbol);
      //15. Quantidade contratada (3 DECIMALES !!!)
      var quantityContrada = ''
      if (arrMap[6] == '06') {
        quantityContrada = arrMap[16];
      }
      arrTempLineas.push(quantityContrada);
      //16. Quantidade medida (3 DECIMALES !!!)
      arrTempLineas.push(quantityContrada);
      //17. Total
      arrTempLineas.push(valorTotal);
      //18. Desconto / Redutores (NO SE CONTEMPLA)
      arrTempLineas.push('');
      //19. Acréscimos e Despesas Acessórias (NO SE CONTEMPLA)
      arrTempLineas.push('');
      //20. BC ICMS
      arrTempLineas.push(baseCalculo);
      //21. ICMS
      arrTempLineas.push(impICMS);
      //22. Operações Isentas ou não tributadas
      arrTempLineas.push(isentas);
      //23. Outros valores
      arrTempLineas.push(outras);
      //24. Alíquota do ICMS
      arrTempLineas.push(alicuotaICMS);
      //25. Situação (COMO SE MANEJAN LAS OTRAS OPCIONES? !!!!)
      var situacaoDocumento = ' ';
      if (arrMap[17] != null && arrMap[17] != '') {
        if (arrMap[17] == 'Cancelada') {
          situacaoDocumento = 'S';
        } else {
          situacaoDocumento = 'N';
        }
      }
      arrTempLineas.push(situacaoDocumento);
      //26. Ano e Mês de referência de apuração
      arrTempLineas.push(arrMap[11]);
      //27. Número do Contrato
      arrTempLineas.push(brContractDetail.name);
      //28. Quantidade faturada 3 decimales
      arrTempLineas.push(quantityContrada);
      //29. Tarifa Aplicada / Preço Médio Efetivo (6 decimales)
      arrTempLineas.push(arrMap[23]);
      //30. Alíquota PIS/PASEP
      arrTempLineas.push(alicuotaPIS);
      //31. PIS/PASEP
      arrTempLineas.push(impPIS);
      //32. Alíquota COFINS
      arrTempLineas.push(alicuotaCOFINS);
      //33. COFINS
      arrTempLineas.push(impCOFINS);
      //34. Indicador de Desconto Judicial//(!!!)
      arrTempLineas.push('');
      //35. Tipo de Isenção/Redução de Base de Cálculo
      if (arrMap[6] == '06') {
        arrTempLineas.push('0');
      } else {
        arrTempLineas.push('');
      }
      //36. Brancos - reservado para uso futuro
      arrTempLineas.push('');
      //37. Código de Autenticação Digital do registro
      arrTempLineas.push('');

      /*CAMPOS ADICIONALES (PARA ARCHIVO MAESTRE)*/
      arrTempLineas.push(arrMap[19]); //38. fecha lectura anterior
      arrTempLineas.push(arrMap[20]); //39. fecha lectura actual
      arrTempLineas.push(arrMap[21]); //40. Número ou código da fatura comercial
      //log.debug('Linea item para enviar a reduce', arrTempLineas);
      return arrTempLineas;
    }

    function lengthInUtf8Bytes(str) {
      var m = encodeURIComponent(str).match(/%[89ABab]/g);
      return str.length + (m ? m.length : 0);
    }

    function actualizarLog(id_carpeta, name_carpeta) {
      var record = recordModulo.load({
        type: 'customrecord_lmry_br_rpt_generator_log',
        id: paramLogID
      });

      var urlfile = '';

      if (name_carpeta == 'error') {
        name_carpeta = "Ocurrio un error inesperado en la ejecucion del reporte.";
      } else {
        if (id_carpeta != '') {
          var host = url.resolveDomain({
            hostType: url.HostType.APPLICATION
          });

          urlfile = 'https://' + host + '/core/media/downloadfolder.nl?id=' + id_carpeta;
          name_carpeta += '.zip';
        } else {
          name_carpeta = "No existe informacion para los criterios seleccionados.";
        }
      }

      //Nombre de Archivo
      record.setValue({
        fieldId: 'custrecord_lmry_br_rg_name_field',
        value: name_carpeta
      });
      //Url de Archivo
      record.setValue({
        fieldId: 'custrecord_lmry_br_rg_url_file',
        value: urlfile
      });

      var recordId = record.save();
      if (urlfile != '') {
        libreria.sendrptuserTranslate(nameReport, 3, name_carpeta, language);
      }

    }

    function obtenerFichero(name_carpeta) {
      var idFichero = '';
      var busqueda_fichero = search.create({
        type: "folder",
        filters: [
          ["name", "is", name_carpeta]
        ],
        columns: [
          search.createColumn({
            name: "internalid",
            label: "Internal ID"
          }),
          search.createColumn({
            name: "foldersize",
            label: "Size (KB)"
          }),
          search.createColumn({
            name: "internalid",
            join: "file",
            label: "ID File"
          })
        ]
      });

      var resultado = busqueda_fichero.run()
      var searchResult = resultado.getRange(0, 1000);

      if (searchResult.length != 0) {
        var arrayIDFiles = [];
        for (var i = 0; i < searchResult.length; i++) {
          var columns = searchResult[i].columns;
          arrayIDFiles.push(searchResult[i].getValue(columns[2]));
        }
        log.debug('Array de archivos preexistentes', arrayIDFiles);
        borrarArchivos(arrayIDFiles);

        idFichero = searchResult[0].getValue('internalid');
        log.debug('ya existe carpeta: ', idFichero);
      } else {
        log.debug('se crea nueva carpeta', idFichero);
        idFichero = crearCarpeta(name_carpeta);
      }

      return idFichero
    }

    function crearCarpeta(name_carpeta) {
      var idFichero = '';

      var folderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_br'
      });

      if (folderId != null && folderId != '') {
        var fichero = recordModulo.create({
          type: 'folder'
        });

        fichero.setValue({
          fieldId: 'name',
          value: name_carpeta
        });

        fichero.setValue({
          fieldId: 'parent',
          value: folderId
        });
        idFichero = fichero.save({
          disableTriggers: true
        });
      }

      return idFichero;
    }

    function obtenerDatosSubsidiaria() {

      var subsi_temp = search.lookupFields({
        type: search.Type.SUBSIDIARY,
        id: paramSubsi,
        columns: ['taxidnum', 'custrecord_lmry_br_uf', 'legalname']
      });

      uf_domicilio = subsi_temp.custrecord_lmry_br_uf;
      companyCNPJ = completar(14, validaNumeros(subsi_temp.taxidnum), true, ' ');
      companyname = subsi_temp.legalname;

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

      var period_temp = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramPeriod,
        columns: ['periodname', 'enddate']
      });

      periodname = period_temp.periodname;
      //Period EndDate
      var periodenddate = period_temp.enddate;

      //Nuevo Formato Fecha
      var parsedDateStringAsRawDateObject = format.parse({
        value: periodenddate,
        type: format.Type.DATE
      });

      mes_date = parsedDateStringAsRawDateObject.getMonth() + 1;
      anio_date = parsedDateStringAsRawDateObject.getFullYear();
      dia_date = parsedDateStringAsRawDateObject.getDate();

      if ((dia_date + '').length == 1) {
        dia_date = '0' + dia_date;
      }
      if ((mes_date + '').length == 1) {
        mes_date = '0' + mes_date;
      }

      if (featureMulti || featureMulti == 'T') {
        var multibookName_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_BOOK,
          id: paramMulti,
          columns: ['name']
        });

        multibookName = multibookName_temp.name;
      }

      //nombre del reporte
      var busqueda_nombre = search.create({
        type: "customrecord_lmry_br_features",
        filters: [
          ["internalid", "anyof", paramFeature]
        ],
        columns: [
          search.createColumn({
            name: "name",
            sort: search.Sort.ASC,
            label: "Name"
          })
        ]
      });

      var auxi = busqueda_nombre.run().getRange(0, 1000);
      nameReport = auxi[0].getValue('name');
    }

    function redondear(number) {
      return Math.round(Number(number) * 100) / 100;
    }

    function formatearLineasItem(arrayLineas, nroItem) {
      var strLineasItem = '';

      //0. CNPJ o CPF
      strLineasItem += completar(14, validaNumeros(arrayLineas[0]), true, '0');
      //1. UF (FALTA CONSIDERAR EXTERIOR)
      strLineasItem += arrayLineas[1];
      //2. Clase de Consumo
      strLineasItem += arrayLineas[2];
      //3. Fase ou Tipo de Utilização
      strLineasItem += arrayLineas[3];
      //4. Grupo de Tensão
      strLineasItem += completar(2, arrayLineas[4], true, '0');
      //5. Data de Emissão
      strLineasItem += arrayLineas[5];
      //6. Modelo
      strLineasItem += arrayLineas[6];
      //7. Série (CONSIDERAR RESTRICCION !!!!!)
      strLineasItem += completar(3, arrayLineas[7], true, '0');
      //8. Número
      strLineasItem += completar(9, arrayLineas[8], true, '0'); //alineado a la derecha
      //9. CFOP
      strLineasItem += validaNumeros(arrayLineas[9]);
      //10. Nº de ordem do Item
      strLineasItem += completar(3, nroItem + 1, true, '0');
      //11. Código do item
      strLineasItem += completar(10, arrayLineas[11], true, '0');
      //12. Descrição do item
      strLineasItem += completar(40, arrayLineas[12], false, ' '); //alineado a la izq
      //13. Código de classificação do item
      strLineasItem += completar(4, arrayLineas[13], true, '0');
      //14. Unidade
      strLineasItem += completar(6, arrayLineas[14], false, ' '); //alineado a la izq
      //15. Quantidade contratada (3 DECIMALES)
      strLineasItem += completar(12, validaNumeros((Number(arrayLineas[15])).toFixed(3)), true, '0');
      //16. Quantidade medida (3 DECIMALES)
      strLineasItem += completar(12, validaNumeros((Number(arrayLineas[16])).toFixed(3)), true, '0');
      //17. Total
      strLineasItem += completar(11, validaNumeros(arrayLineas[17]), true, '0');
      //18. Desconto / Redutores (NO SE CONTEMPLA)
      strLineasItem += completar(11, arrayLineas[18], true, '0');
      //19. Acréscimos e Despesas Acessórias (NO SE CONTEMPLA)
      strLineasItem += completar(11, arrayLineas[19], true, '0');
      //20. BC ICMS
      strLineasItem += completar(11, validaNumeros(arrayLineas[20]), true, '0');
      //21. ICMS
      strLineasItem += completar(11, validaNumeros(arrayLineas[21]), true, '0');
      //22. Operações Isentas ou não tributadas
      strLineasItem += completar(11, validaNumeros(arrayLineas[22]), true, '0');
      //23. Outros valores
      strLineasItem += completar(11, validaNumeros(arrayLineas[23]), true, '0');
      //24. Alíquota do ICMS
      strLineasItem += completar(4, validaNumeros((arrayLineas[24]).toFixed(2)), true, '0');
      //25. Situação
      strLineasItem += arrayLineas[25];
      //26. Ano e Mês de referência de apuração
      strLineasItem += arrayLineas[26];
      //27. Número do Contrato
      if (arrayLineas[6] == '06') {
        strLineasItem += completar(15, '', true, ' ');
      } else {
        strLineasItem += completar(15, arrayLineas[27], true, '0');
      }
      //28. Quantidade faturada
      strLineasItem += completar(12, validaNumeros((Number(arrayLineas[28])).toFixed(3)), true, '0'); //3 decimales
      //29. Tarifa Aplicada / Preço Médio Efetivo
      strLineasItem += completar(11, validaNumeros(arrayLineas[29]), true, '0');
      //30. Alíquota PIS/PASEP
      strLineasItem += completar(6, validaNumeros((arrayLineas[30]).toFixed(4)), true, '0'); //4 decimales
      //31. PIS/PASEP
      strLineasItem += completar(11, validaNumeros(arrayLineas[31]), true, '0');
      //32. Alíquota COFINS
      strLineasItem += completar(6, validaNumeros((arrayLineas[32]).toFixed(4)), true, '0'); //4 decimales
      //33. COFINS
      strLineasItem += completar(11, validaNumeros(arrayLineas[33]), true, '0');
      //34. Indicador de Desconto Judicial//(!!!)
      strLineasItem += completar(1, arrayLineas[34], true, ' ');
      //35. Tipo de Isenção/Redução de Base de Cálculo (!!!)
      strLineasItem += completar(2, arrayLineas[35], true, '0');
      //36. Brancos - reservado para uso futuro
      strLineasItem += completar(5, arrayLineas[36], true, ' ');
      //37. Código de Autenticação Digital do registro
      var token = md5.md5_general(strLineasItem);
      strLineasItem += token;

      return strLineasItem
    }

    function borrarArchivos(arreglo) {
      for (var i = 0; i < arreglo.length; i++) {
        fileModulo.delete({
          id: arreglo[i]
        });
      }
    }

    function obtenerLineasItem(voided) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = [];

      var savedsearch = search.load({
        /*LatamReady - BR NFCe Sefaz*/
        id: 'customsearch_lmry_br_nfce_sefaz'
      });
      var formula = "CASE WHEN {custbody_lmry_document_type.custrecord_lmry_codigo_doc} = '" + paramModeloNF + "' THEN 1 ELSE 0 END";
      log.debug('formula', formula);

      var voidedFilter = search.createFilter({
        name: 'voided',
        operator: search.Operator.IS,
        values: voided
      });
      savedsearch.filters.push(voidedFilter);

      var modeloNFfilter = search.createFilter({
        name: 'formulatext',
        operator: search.Operator.IS,
        formula: formula,
        values: 1
      });
      savedsearch.filters.push(modeloNFfilter);

      if (featureSubs) {
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
        //24
        var exchangeRateMultibook = search.createColumn({
          name: 'exchangerate',
          join: 'accountingtransaction'
        });
        savedsearch.columns.push(exchangeRateMultibook);
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

            // 0. Line Unique Key
            arr[0] = objResult[i].getValue(columns[0]);
            // 1. ID Transaction
            arr[1] = objResult[i].getValue(columns[1]);
            // 2. Customer ID
            arr[2] = objResult[i].getValue(columns[2])
            // 3. UF
            arr[3] = objResult[i].getValue(columns[3]);
            // 4. ID Contract Detail Energy
            arr[4] = objResult[i].getValue(columns[4]);
            // 5. Fecha Emision
            arr[5] = objResult[i].getValue(columns[5]);
            // 6. Modelo Nota Fiscal
            arr[6] = objResult[i].getValue(columns[6]);
            // 7. Serie
            arr[7] = objResult[i].getValue(columns[7]);
            // 8. Numero NF
            arr[8] = objResult[i].getValue(columns[8]);
            // 9. Amount item
            if (featureMulti) {
              arr[9] = (redondear(objResult[i].getValue(columns[9]) / objResult[i].getValue(columns[12]) * objResult[i].getValue(columns[24]))).toFixed(2);
            } else {
              arr[9] = (redondear(objResult[i].getValue(columns[9]))).toFixed(2);
            }
            // 10. CST
            arr[10] = objResult[i].getValue(columns[10]);
            // 11. Periodo Referencia
            arr[11] = objResult[i].getValue(columns[11]);
            // 12. Tipo Cambio
            if (featureMulti) {
              arr[12] = objResult[i].getValue(columns[24]);
            } else {
              arr[12] = objResult[i].getValue(columns[12]);
            }
            // 13. ID Catalog Item
            arr[13] = objResult[i].getValue(columns[13]);
            // 14. CFOP
            arr[14] = objResult[i].getValue(columns[14]);
            // 15. ITEM ID
            arr[15] = objResult[i].getValue(columns[15]);
            // 16. Quantity
            arr[16] = objResult[i].getValue(columns[16]);
            // 17. Status Document
            var eiStatus = objResult[i].getValue(columns[17]);
            if (eiStatus == null || eiStatus == '' || eiStatus == '- None -') {
              voided ? arr[17] = 'Cancelada' : arr[17] = 'No Cancelada'
            } else {
              arr[17] = eiStatus;
            }
            // 18. ID Contract Detail Comunicaciones
            arr[18] = objResult[i].getValue(columns[18]);
            // 19. Fecha lectura anterior
            arr[19] = objResult[i].getValue(columns[19]);
            // 20. Fecha lectura actual
            arr[20] = objResult[i].getValue(columns[20]);
            // 21. Número ou código da fatura comercial
            arr[21] = objResult[i].getValue(columns[21]);
            // 22. Valor total de la factura comercial
            arr[22] = objResult[i].getValue(columns[22]);
            // 23. Tarifa Aplicada / Preço Médio Efetivo (com 6 decimais)
            if (objResult[i].getValue(columns[23]) != null) {
              if (featureMulti) {
                arr[23] = (objResult[i].getValue(columns[23]) / objResult[i].getValue(columns[12]) * objResult[i].getValue(columns[24])).toFixed(6);
              } else {
                arr[23] = (objResult[i].getValue(columns[23])).toFixed(6);
              }
            } else {
              arr[23] = 0;
            }

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

    function validaNumeros(s) {
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

    function obtenerTaxResults(transactionID, lineUniqueKey) {
      var DbolStop = false;
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var ArrReturn = [];

      var savedsearch = search.create({
        type: "customrecord_lmry_br_transaction",
        filters: [
          ["custrecord_lmry_br_transaction", "is", transactionID],
          "AND",
          ["custrecord_lmry_lineuniquekey", "equalto", lineUniqueKey]
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
            arr[0] = objResult[i].getValue(columns[0]);
            // 1. Imposto
            arr[1] = objResult[i].getValue(columns[1]);
            // 2. Percent
            arr[2] = objResult[i].getValue(columns[2]) * 10000;
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

    function completar(long, valor, orientacion, caracter) {
      if (valor == null) {
        valor = '';
      }

      if (('' + valor).length <= long) {
        if (long != ('' + valor).length) {
          for (var i = ('' + valor).length; i < long; i++) {
            if (orientacion) {
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
        valor = (valor + '').substring(0, long);
        return valor;
      }
    }

    function nameFile(name_file, tipo, secuencia) {
      var tipoArchivo = 'D';
      if (tipo == 'item') {
        tipoArchivo = 'I'
      }
      if (tipo == 'maestre') {
        tipoArchivo = 'M'
      }

      var secuenciaArc = completar(3, secuencia, true, '0');
      var str = name_file + tipoArchivo + '.' + secuenciaArc;
      return str;
    }

    function saveFile(folderId, strContenido, name_file, tipo, secuencia) {
      //CR/CF
      strContenido += '\r\n';
      // Extension del archivo
      var name = nameFile(name_file, tipo, secuencia);
      // Crea el archivo
      var file = fileModulo.create({
        name: name,
        fileType: fileModulo.Type.PLAINTEXT,
        contents: strContenido,
        encoding: fileModulo.Encoding.ISO_8859_1,
        folder: folderId
      });

      var idfile = file.save();
    }

    return {
      getInputData: getInputData,
      map: map,
      reduce: reduce,
      summarize: summarize
    };

  });
