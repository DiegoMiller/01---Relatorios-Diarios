(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$NfAjustadas={7158434776,31805518557,718215115795,2718325399674,14900554525,25163365209,324174858,31805518570,7170189979,224174856,31805518558,924174855,272340605972,2418201212770,291023071157,2538358179,33104468932,104600137,104600716,25163864791,22166125807,201031374203,1037807156,1037807444,251042583800,1046002185,2010358517051,615675156440,215675157571,24103988176055,11031374203,315675156440,315675157571,9166125807,131042583800,1810358517051,2038358179,23163864791,25103988176055,31805518557,31805518558,31805518570,181037807156,201037807156,221037807444,271037807444,23104600137,24104600137,12104600716,13104600716,181046002185,271046002185,2910357615689,3310357615689,2910357615690,3310357615690,2910357615691,3310357615691,2102628162719,31102628162719,211046001287,231046001287,171805520810,201805520810,181025314239,231025314239,17154711923,17154711924,251762727065,291762727065,1103171307370,18103171307370,31035859696,234752688,2734752688,9175039643,31175039643,271035853305,291035853305};


$NfDAjustadas={1163844176,131035856752,33176994,121035855753,291035852152,221035851743,33176995,33176996,1163863859,1158813963,1158813964,1163863858,11039883989,319993166,319993168,11039883990,319993167,3163863342,71588124698,71588124699,71805525814,61020982149,171047351772,131034626100,131034626098,211035851663,181035852226,181035852429,181035852225,211035851662,181035852427,22158661610,22158661638,22158661611,22158661639,221031711435,241251722,221031711436,241251717,30158662195,31180551354,301003082467,31180551355,301003082183,291031651930,301035852410,291031651929,301035852409,61020982148,3163863339,71805525818,710262825785,91032383999,61035852057,710358524875,710262825786,91032384000,61035852058,710358524876,1716191732,12144185102,710420624900,12144185103,24158661537,241251720,24180551502,24180551546,27180552037,24158661538,24158661582,24180551500,27506331839,241003081592,241003081645,241003081590,291016831399,27180552492,27506331840,241003081646,271035851805,271035851807,24158661583,24180551501,24180551545,27180552034,27180552493,291016831398,1716191731,12158665125,121015975155,121035854985,121035854986,121015975154,710420624902,12158665124,2016191757,2016191756,171047351771,31180551356,30158662194,301003082185,301003082462,301035852175,301035852535,301035852174,301035852534,9180554307,9180554308,9180554309,9180554316,9180554317,25180551830,25180551831,2180553060,2180553063,29180552226,29180552289,29180552227,29180552286,29180552287,29180552288,311035851381,311035851382,121035856190,121035856192,27180552657,27180552658,3180553784,3180553785,22158661914,22158661915,3158663816,3158663817,9158664525,9158664526,17154711923,17154711924,71586626117,71586626118,181035852712,181035852713,22158661951,22158661952,311048081624,311048081628,1182134633,1182134634,271017132793,271017132795,23158661785,23158661786,23158661787,23158661788,68942491,68942492,710358526509,710358526510};
$lojas={1,2,3,6,7,8,9,12,13,17,18,20,21,22,23,24,25,27,29,30,31,33,9999};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["marche.png"];
SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};


getALIMENTUM[codLoja_,NfAjustadas_]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 
	
SET NOCOUNT ON;	
		
-- =============================================
-- Autor		      : Diego Miller
-- Data de Cria\[CCedilla]\[ATilde]o    : 18/02/2016
-- Descri\[CCedilla]\[ATilde]o          : Entradas Duplicadas no Sistema Zeus
-- Bancos Utilizados  : Zeus_RTG e BI
-- Datas de Alteracao : --/--/--
-- =============================================

---------------------------------------------------------------------------
--ENTRADAS DOS ULTIMOS 120 DIAS--
---------------------------------------------------------------------------

SELECT DISTINCT 
		COD_LOJA
		,COD_FORNECEDOR
		,CONVERT(DATE,DTA_EMISSAO) DTA_EMISSAO 
		,CONVERT(DATE,DTA_ENTRADA) DTA_ENTRADA 
		,NUM_NF_FORN 
INTO   #TMP_ENTRADAS_POS_INV
FROM [ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS]  WITH (NOLOCK) 

WHERE 1=1 
AND CONVERT(DATE,DTA_ENTRADA) >= CONVERT(DATE,'2015-07-01') 

---------------------------------------------------------------------------
--MONTANDO RESUMO--
---------------------------------------------------------------------------

SELECT TOP 20
		COD_FORNECEDOR  CodForn
		,DES_FORNECEDOR Forn
		,CONVERT(VARCHAR,DTA_EMISSAO,103) Data
		,DATEDIFF(DD,DTA_EMISSAO,GETDATE()) [Dias]
		--,NUM_NF_FORN NF
FROM 
(
				SELECT 
					TMP.COD_FORNECEDOR 
					,DES_FORNECEDOR 
					,DTA_EMISSAO 
					,NUM_NF_FORN				
				 FROM  
				(
						SELECT 
							COD_FORNECEDOR
							,NUM_NF_FORN
							,DTA_EMISSAO
							,COUNT(DTA_ENTRADA) CONTAGEM  
						FROM #TMP_ENTRADAS_POS_INV INV
						WHERE 1=1
								AND CAST(CONVERT(VARCHAR,COD_LOJA) + CONVERT(VARCHAR,COD_FORNECEDOR) + CONVERT(VARCHAR,NUM_NF_FORN) AS DOUBLE PRECISION) NOT IN (`2`)
						GROUP BY 
							COD_FORNECEDOR
							,NUM_NF_FORN 
							,DTA_EMISSAO
				) TMP 
					LEFT JOIN TAB_FORNECEDOR FORN 
						ON FORN.COD_FORNECEDOR = TMP.COD_FORNECEDOR
				WHERE 1=1 
						AND CONTAGEM > 1 
) TMP2
WHERE 1=1
	AND DTA_EMISSAO > '2016-01-01'

ORDER BY DTA_EMISSAO ASC

DROP TABLE #TMP_ENTRADAS_POS_INV

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLArgument@@NfAjustadas}];
	r
];
r\[LeftArrow]getALIMENTUM[$lojas,$NfAjustadas]


grid8[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
	 #>7,Red
	,True,Darker@Green
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"Dias"}-> stl},"TotalRow"->False];
	title=Style[Row@{"Top Entradas Duplicadas"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
grid8[r]


getExcel[codLoja_,NfAjustadas_]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 
	

SET NOCOUNT ON;	
		
-- =============================================
-- Autor		      : Diego Miller
-- Data de Cria\[CCedilla]\[ATilde]o    : 18/02/2016
-- Descri\[CCedilla]\[ATilde]o          : Entradas Duplicadas no Sistema Zeus
-- Bancos Utilizados  : Zeus_RTG e BI
-- Datas de Alteracao : --/--/--
-- =============================================

---------------------------------------------------------------------------
--ENTRADAS DOS ULTIMOS 120 DIAS--
---------------------------------------------------------------------------

SELECT DISTINCT 
		COD_LOJA
		,COD_FORNECEDOR
		,CONVERT(DATE,DTA_EMISSAO) DTA_EMISSAO 
		,CONVERT(DATE,DTA_ENTRADA) DTA_ENTRADA 
		,NUM_NF_FORN 
INTO   #TMP_ENTRADAS_POS_INV
FROM [ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS]  WITH (NOLOCK) 

WHERE 1=1 
AND CONVERT(DATE,DTA_ENTRADA) >= CONVERT(DATE,'2015-07-01') 
--AND DES_ESPECIE NOT IN ('NFD')

---------------------------------------------------------------------------
--MONTANDO RESUMO--
---------------------------------------------------------------------------

SELECT 
		COD_FORNECEDOR
		,DES_FORNECEDOR 
		,CONVERT(date,DTA_EMISSAO)  DTA_EMISSAO
		,NUM_NF_FORN NF
		INTO #TMP_NF_FORN
FROM 
(
				SELECT 
					TMP.COD_FORNECEDOR 
					,DES_FORNECEDOR 
					,DTA_EMISSAO 
					,NUM_NF_FORN				
				 FROM  
				(
						SELECT 
							COD_FORNECEDOR
							,NUM_NF_FORN
							,DTA_EMISSAO
							,COUNT(DTA_ENTRADA) CONTAGEM  
						FROM #TMP_ENTRADAS_POS_INV INV
						WHERE 1=1
								AND CAST(CONVERT(VARCHAR,COD_LOJA) + CONVERT(VARCHAR,COD_FORNECEDOR) + CONVERT(VARCHAR,NUM_NF_FORN) AS DOUBLE PRECISION) NOT IN (`2`)
						GROUP BY 
							COD_FORNECEDOR
							,NUM_NF_FORN 
							,DTA_EMISSAO
				) TMP 
					LEFT JOIN TAB_FORNECEDOR FORN 
						ON FORN.COD_FORNECEDOR = TMP.COD_FORNECEDOR
				WHERE 1=1 
						AND CONTAGEM > 1 
) TMP2
WHERE 1=1
	AND DTA_EMISSAO > '2016-01-01'

SELECT DISTINCT
		COD_LOJA
		,DUPLICADAS.COD_FORNECEDOR
		,DUPLICADAS.DES_FORNECEDOR 
		,CONVERT(DATE,DUPLICADAS.DTA_EMISSAO)  DTA_EMISSAO
		,NF
FROM 
#TMP_NF_FORN DUPLICADAS
LEFT JOIN [ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS] ENTRADAS
ON (ENTRADAS.COD_FORNECEDOR = DUPLICADAS.COD_FORNECEDOR AND ENTRADAS.DTA_EMISSAO = DUPLICADAS.DTA_EMISSAO AND ENTRADAS.NUM_NF_FORN = DUPLICADAS.NF)
ORDER BY 
		CONVERT(DATE,DUPLICADAS.DTA_EMISSAO) , NF


DROP TABLE #TMP_ENTRADAS_POS_INV
DROP TABLE #TMP_NF_FORN

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLArgument@@NfAjustadas}];
	r
];
r3\[LeftArrow]getExcel[$lojas,$NfAjustadas]


$fileNameExcelNF="Rela\[CCedilla]\[ATilde]o de NF's "<>$now2<>".xlsx";
Export["Rela\[CCedilla]\[ATilde]o de NF's "<>$now2<>".xlsx",r3["DataAll"]];		


getExcelNFDduplicada[codLoja_,NfDAjustadas_]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 
	

SET NOCOUNT ON;	


SET NOCOUNT ON;	
		
-- =============================================
-- AUTOR		      : DIEGO MILLER
-- DATA DE CRIA\[CapitalCCedilla]\[CapitalATilde]O    : 18/02/2016
-- DESCRI\[CapitalCCedilla]\[CapitalATilde]O          : ENTRADAS DUPLICADAS NO SISTEMA ZEUS
-- BANCOS UTILIZADOS  : ZEUS_RTG E BI
-- DATAS DE ALTERACAO : --/--/--
-- =============================================

SELECT 
		COD_LOJA
		,CONVERT(DATE,DTA_EMISSAO) AS DATA
		,NFD.COD_FORNECEDOR
		,DES_FORNECEDOR
		,NUM_NF_FORN 
		,VAL_TOTAL_NF
INTO   #TMP_NFD
FROM 
TAB_FORNECEDOR_NOTA NFD
	LEFT JOIN TAB_FORNECEDOR FORN
		ON (FORN.COD_FORNECEDOR = NFD.COD_FORNECEDOR)
		
WHERE 1=1
		  AND DES_ESPECIE = 'NFD'  
		  AND DTA_EMISSAO >=  '2016-01-01'
		  AND COD_FISCAL NOT IN (42)
		  AND CAST(CONVERT(VARCHAR,COD_LOJA) + CONVERT(VARCHAR,NFD.COD_FORNECEDOR) + CONVERT(VARCHAR,NUM_NF_FORN) AS DOUBLE PRECISION) NOT IN (`2`)		
		  

SELECT  COD_LOJA 
		,COD_FORNECEDOR 
		,DES_FORNECEDOR 
		,CONVERT(DATE,DATA) DATA_EMISSAO
		,VAL_TOTAL_NF 
INTO #TMP_NFD_DUPLICADAS
FROM (
SELECT 
		DISTINCT COD_LOJA
		,DATA
		,COD_FORNECEDOR
		,DES_FORNECEDOR
		,VAL_TOTAL_NF
		,COUNT(NUM_NF_FORN) DUPLICIDADE
FROM #TMP_NFD 
WHERE 1=1 
		AND COD_LOJA IN (`1`)

GROUP BY 
		COD_LOJA
		,DATA
		,COD_FORNECEDOR
		,DES_FORNECEDOR
		,VAL_TOTAL_NF

) TMP 
WHERE DUPLICIDADE > 1



SELECT 
 E.COD_LOJA 
		,E.COD_FORNECEDOR 
		,E.DES_FORNECEDOR 
		,CONVERT(DATE,E.DATA_EMISSAO) DATA_EMISSAO
		,NFD.NUM_NF_FORN
		,E.VAL_TOTAL_NF 

 FROM #TMP_NFD_DUPLICADAS E
LEFT JOIN TAB_FORNECEDOR_NOTA NFD
ON (NFD.COD_LOJA = E.COD_LOJA AND NFD.COD_FORNECEDOR = E.COD_FORNECEDOR AND CONVERT(DATE,NFD.DTA_EMISSAO) = CONVERT(DATE,E.DATA_EMISSAO) AND NFD.VAL_TOTAL_NF = E.VAL_TOTAL_NF)
WHERE 1=1

		  AND DES_ESPECIE = 'NFD'  
		  AND NFD.DTA_EMISSAO >=  '2016-01-01'
		  AND COD_FISCAL NOT IN (42)

ORDER BY CONVERT(DATE,E.DATA_EMISSAO), E.VAL_TOTAL_NF 


DROP TABLE #TMP_NFD_DUPLICADAS
DROP TABLE #TMP_NFD
		
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLArgument@@NfDAjustadas}];
	r
];
r40\[LeftArrow]getExcelNFDduplicada[$lojas,$NfDAjustadas]


$fileNameExcelNFD="NFD Duplicadas em "<>$now2<>".xlsx";
Export["NFD Duplicadas em "<>$now2<>".xlsx",r40["DataAll"]];		


getTOTAL[codLoja_,NfAjustadas_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 
	
SET NOCOUNT ON;	
		
-- =============================================
-- AUTOR		      : DIEGO MILLER
-- DATA DE CRIA\[CapitalCCedilla]\[CapitalATilde]O    : 18/02/2016
-- DESCRI\[CapitalCCedilla]\[CapitalATilde]O          : ENTRADAS DUPLICADAS NO SISTEMA ZEUS
-- BANCOS UTILIZADOS  : ZEUS_RTG E BI
-- DATAS DE ALTERACAO : --/--/--
-- =============================================

---------------------------------------------------------------------------
--ENTRADAS DOS ULTIMOS 120 DIAS--
---------------------------------------------------------------------------

SELECT DISTINCT 
		COD_LOJA
		,COD_FORNECEDOR
		,CONVERT(DATE,DTA_EMISSAO) DTA_EMISSAO 
		,CONVERT(DATE,DTA_ENTRADA) DTA_ENTRADA 
		,NUM_NF_FORN 
INTO   #TMP_ENTRADAS_POS_INV
FROM [ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS]  WITH (NOLOCK) 

WHERE 1=1 
AND CONVERT(DATE,DTA_ENTRADA) >= CONVERT(DATE,'2015-07-01') 
--AND DES_ESPECIE NOT IN ('NFD')

---------------------------------------------------------------------------
--MONTANDO RESUMO--
---------------------------------------------------------------------------

SELECT 

		count(*) as Qtde
FROM 
(
				SELECT 
					TMP.COD_FORNECEDOR 
					,DES_FORNECEDOR 
					,DTA_EMISSAO 
					,NUM_NF_FORN				
				 FROM  
				(
						SELECT 
							COD_FORNECEDOR
							,NUM_NF_FORN
							,DTA_EMISSAO
							,COUNT(DTA_ENTRADA) CONTAGEM  
						FROM #TMP_ENTRADAS_POS_INV INV
						WHERE 1=1
								AND CAST(CONVERT(VARCHAR,COD_LOJA) + CONVERT(VARCHAR,COD_FORNECEDOR) + CONVERT(VARCHAR,NUM_NF_FORN) AS DOUBLE PRECISION) NOT IN (`2`)
						GROUP BY 
							COD_FORNECEDOR
							,NUM_NF_FORN 
							,DTA_EMISSAO
				) TMP 
					LEFT JOIN TAB_FORNECEDOR FORN 
						ON FORN.COD_FORNECEDOR = TMP.COD_FORNECEDOR
				WHERE 1=1 
						AND CONTAGEM > 1 
) TMP2
WHERE 1=1
	AND DTA_EMISSAO > '2016-01-01'
DROP TABLE #TMP_ENTRADAS_POS_INV


";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLArgument@@NfAjustadas}];
	$Resultado=ToString@r["Data"][[1,1]]

];
r1\[LeftArrow]getTOTAL[$lojas,$NfAjustadas]


getNFDTOTAL[codLoja_,NfDAjustadas_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 
	
SET NOCOUNT ON;	
		
-- =============================================
-- AUTOR		      : DIEGO MILLER
-- DATA DE CRIA\[CapitalCCedilla]\[CapitalATilde]O    : 18/02/2016
-- DESCRI\[CapitalCCedilla]\[CapitalATilde]O          : ENTRADAS DUPLICADAS NO SISTEMA ZEUS
-- BANCOS UTILIZADOS  : ZEUS_RTG E BI
-- DATAS DE ALTERACAO : --/--/--
-- =============================================

SELECT 
		COD_LOJA
		,CONVERT(DATE,DTA_EMISSAO) AS DATA
		,NFD.COD_FORNECEDOR
		,DES_FORNECEDOR
		,NUM_NF_FORN 
		,VAL_TOTAL_NF
INTO   #TMP_NFD
FROM 
TAB_FORNECEDOR_NOTA NFD
	LEFT JOIN TAB_FORNECEDOR FORN
		ON (FORN.COD_FORNECEDOR = NFD.COD_FORNECEDOR)
		
WHERE 1=1
		  AND DES_ESPECIE = 'NFD'  
		  AND DTA_EMISSAO >=  '2016-01-01'
		  AND COD_FISCAL NOT IN (42)
          AND CAST(CONVERT(VARCHAR,COD_LOJA) + CONVERT(VARCHAR,NFD.COD_FORNECEDOR) + CONVERT(VARCHAR,NUM_NF_FORN) AS DOUBLE PRECISION) NOT IN (`2`)		


SELECT count(*) NFD_DUPLICIDADES FROM (
SELECT 
		DISTINCT COD_LOJA
		,DATA
		,COD_FORNECEDOR
		,DES_FORNECEDOR
		,VAL_TOTAL_NF
		,COUNT(NUM_NF_FORN) DUPLICIDADE
FROM #TMP_NFD 
where 1=1 and cod_loja in (`1`)


GROUP BY 
		COD_LOJA
		,DATA
		,COD_FORNECEDOR
		,DES_FORNECEDOR
		,VAL_TOTAL_NF

) TMP 
WHERE DUPLICIDADE > 1


";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLArgument@@NfDAjustadas}];
	$ResultadoNFD=ToString@r["Data"][[1,1]]

];
r20\[LeftArrow]getNFDTOTAL[$lojas,$NfDAjustadas];


getNFDduplicada[codLoja_,NfDAjustadas_]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 
	

SET NOCOUNT ON;	
		
-- =============================================
-- Autor		      : Diego Miller
-- Data de Cria\[CCedilla]\[ATilde]o    : 18/02/2016
-- Descri\[CCedilla]\[ATilde]o          : Entradas Duplicadas no Sistema Zeus
-- Bancos Utilizados  : Zeus_RTG e BI
-- Datas de Alteracao : --/--/--
-- =============================================

SELECT 
		COD_LOJA
		,CONVERT(DATE,DTA_EMISSAO) AS DATA
		,NFD.COD_FORNECEDOR
		,DES_FORNECEDOR
		,NUM_NF_FORN 
		,VAL_TOTAL_NF
INTO   #TMP_NFD
FROM 
TAB_FORNECEDOR_NOTA NFD
	LEFT JOIN TAB_FORNECEDOR FORN
		ON (FORN.COD_FORNECEDOR = NFD.COD_FORNECEDOR)
		
WHERE 1=1
		  AND DES_ESPECIE = 'NFD'  
		  AND DTA_EMISSAO >=  '2016-01-01'
		  AND COD_FISCAL NOT IN (42)
          AND CAST(CONVERT(VARCHAR,COD_LOJA) + CONVERT(VARCHAR,NFD.COD_FORNECEDOR) + CONVERT(VARCHAR,NUM_NF_FORN) AS DOUBLE PRECISION) NOT IN (`2`)		


SELECT TOP 20 
		COD_FORNECEDOR CodForn
		,DES_FORNECEDOR Forn
		--COD_LOJA Loja
		,convert(varchar,DATA,103) Data
		,datediff(DD,DATA,getdate()) Dias
	--	,VAL_TOTAL_NF [Vlr NFD]
FROM (
SELECT 
		DISTINCT COD_LOJA
		,DATA
		,COD_FORNECEDOR
		,DES_FORNECEDOR
		,VAL_TOTAL_NF
		,COUNT(NUM_NF_FORN) DUPLICIDADE
FROM #TMP_NFD 
WHERE 1=1 
		and cod_loja in (`1`)

GROUP BY 
		COD_LOJA
		,DATA
		,COD_FORNECEDOR
		,DES_FORNECEDOR
		,VAL_TOTAL_NF

) TMP 
WHERE DUPLICIDADE > 1
ORDER BY convert(date,DATA) ASC

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLArgument@@NfDAjustadas}];
	r
];
r10\[LeftArrow]getNFDduplicada[$lojas,$NfDAjustadas]


gridDuplicadas[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	
	color1=Which[ 
	 #>7,Red
	,True,Darker@Green
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color1@val];
	stl[Null]="0"; 

	color2=Which[ 
	 #>7,Black
	,True,Darker@Green
	]&;

    stl2[val_]:=Style[maF["N,00"][val],color2@val];
	stl2[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"Dias"}-> stl},"TotalRow"->False];
	title=Style[Row@{"Top NFD Duplicadas"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridDuplicadas[r10];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{grid8[r],gridDuplicadas[r10]}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];
rep=marcheTemplate[grid=Row@{"Auditoria NF & NFD Duplicada Zeus ",$now},createReport[s],1200, $logo];


$fileName1="Auditoria NF & NFD Duplicada Zeus "<>$now2<>".png";
Export["Auditoria NF & NFD Duplicada Zeus "<>$now2<>".png",rep];


marcheMail[
  		"[GE] Existe(em) " <>$Resultado<> " NF duplicada(as) e " <>$ResultadoNFD<> " NFD duplicada(as) em " <>$now
  		,"Auditoria de NF e NFD Duplicadas

NF Duplicada - Verificar se a nota pode ser excluida, caso n\[ATilde]o, solicitar corre\[CCedilla]\[ATilde]o de estoque.
NFD Duplicada - Verificar se a nota pode ser cancelada, caso n\[ATilde]o, solicitar corre\[CCedilla]\[ATilde]o de estoque."
		  ,$mailsGerencia
		  ,{$fileName1,$fileNameExcelNF, $fileNameExcelNFD}	
]
