(* ::Package:: *)

(* ::Section:: *)
(*ESTOQUE DIA FRIOS*)


Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$lojas={1,2,3,6,7,9,12,13,17,18,20,21,22,23,24,25,27,29,30,31};
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];


getDNV[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 

	SET NOCOUNT ON;

SELECT 
		COD_LOJA
		,COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 QTD_AJUSTE
		INTO #TEMP_AJUSTE
 FROM 
		[192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE WITH (NOLOCK)
WHERE 1=1 
AND COD_AJUSTE = 119 
AND COD_PRODUTO IN (55864,148375,385510,505529,552080,552271,882965)
AND CONVERT(DATE,DTA_AJUSTE) >= '20150701'
GROUP BY 
	COD_LOJA 
	,COD_PRODUTO
SELECT	
		COD_LOJA
		,COD_PRODUTO
		,SUM(CAST(QTDE AS DOUBLE PRECISION)) QTDE
		INTO #TEMP_VENDA
FROM 
		DW.DBO.BI_ANAL_TICKET WITH (NOLOCK)
WHERE 1=1 
AND CONVERT(DATE,DATA) >= '20151201'
AND COD_PRODUTO = 1030619

GROUP BY 
	COD_LOJA 
	,COD_PRODUTO


SELECT COD_LOJA, CONVERT(INT,'1030619') COD_PRODUTO, (SUM(QTD_AJUSTE)/7) QTD_AJUSTE  
INTO   #TMP_REUSMO
FROM #TEMP_AJUSTE
GROUP BY COD_LOJA


SELECT 
		V.COD_LOJA LOJA
		,V.COD_PRODUTO PLU
		,QTDE VENDA
		,ISNULL(QTD_AJUSTE,0) AJUSTE
		,QTDE-ISNULL(QTD_AJUSTE,0) DIF	
FROM #TEMP_VENDA V LEFT JOIN #TMP_REUSMO A ON A.COD_LOJA = V.COD_LOJA
AND V.COD_LOJA IN (`1`)

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
]
r[99]=getDNV[$lojas];


gridDNV[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
			#<0.04,Darker@Green
			,#<0.06,Orange,
			,True,Red
	]&;

	stl[val_]:=Style[maF["%"][val],Bold,color2@val];
	stl[Null]="";

	grid=maReportGrid[r,"ColumnFormat"->{{"VENDA","AJUSTE","DIF"}-> "N"},"TotalRow"->False];
	title=Style[Row@{"Controle de Ajuste ", \!\(\*
StyleBox[
TemplateBox[{"$now"},
"RowDefault"],
StripOnInput->False,
LineColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
FrontFaceColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
BackFaceColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
GraphicsColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
FontFamily->"Helvetica",
FontWeight->Bold,
FontColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`]]\)},maTitleFormatOptions];
	(maReportFrame[title,grid])
]
gridDNV[r[99]];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

$fileName=ToString@Row@{"Controle de Ajuste Kit Rabanada "<>$now2<>".png"};
Export["Controle de Ajuste Kit Rabanada "<>$now2<>".png",gridDNV[r[99]]];


getExcel[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 

	SET NOCOUNT ON;
SELECT 
		COD_LOJA
		,COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 QTD_AJUSTE
		INTO #TEMP_AJUSTE
 FROM 
		[192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE WITH (NOLOCK)
WHERE 1=1 
AND COD_AJUSTE = 119 
AND COD_PRODUTO IN (55864,148375,385510,505529,552080,552271,882965)
AND CONVERT(DATE,DTA_AJUSTE) >= '20150701'
GROUP BY 
	COD_LOJA 
	,COD_PRODUTO
SELECT	
		COD_LOJA
		,COD_PRODUTO
		,SUM(CAST(QTDE AS DOUBLE PRECISION)) QTDE
		INTO #TEMP_VENDA
FROM 
		DW.DBO.BI_ANAL_TICKET WITH (NOLOCK)
WHERE 1=1 
AND CONVERT(DATE,DATA) >= '20151201'
AND COD_PRODUTO = 1030619

GROUP BY 
	COD_LOJA 
	,COD_PRODUTO


SELECT COD_LOJA, CONVERT(INT,'1030619') COD_PRODUTO, (SUM(QTD_AJUSTE)/7) QTD_AJUSTE  
INTO   #TMP_REUSMO
FROM #TEMP_AJUSTE
GROUP BY COD_LOJA


SELECT 
		V.COD_LOJA
		,V.COD_PRODUTO
		,QTDE VENDA
		,ISNULL(QTD_AJUSTE,0) AJUSTE
		,QTDE-ISNULL(QTD_AJUSTE,0) DIF
INTO #FINAL		
FROM #TEMP_VENDA V LEFT JOIN #TMP_REUSMO A ON A.COD_LOJA = V.COD_LOJA


SELECT
	A.COD_LOJA
	,A.COD_PRODUTO
	,DIF AJUSTE
FROM 
BI.DBO.BI_LINHA_PRODUTOS A INNER JOIN #FINAL B ON B.COD_LOJA = A.COD_LOJA
WHERE 1=1
AND A.COD_PRODUTO IN (55864,148375,385510,505529,552080,552271,882965)
AND DIF <> 0
AND B.COD_LOJA IN (`1`)

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
]
r=getExcel[$lojas];


$Ajustes=Export["Controle de Ajuste Kit Rabanada "<>$now2<>".xlsx",r["DataAll"]];


marcheMail[
  		"Controle de Ajuste Kit Rabanada " <>$now
  		,""
  		,$mailsGerencia(*{"cleberton.silva@marche.com.br","diego.miller@marche.com.br"}*)
  		,{$fileName, $Ajustes}
  ];
