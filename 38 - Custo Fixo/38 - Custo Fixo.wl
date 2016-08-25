(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$DtaIni={2016,06,01};


SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["marche.png"];
SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};


getExcel[DtaIni_]:=Module[{sql,r,conn=marcheConn2[],tab},
sql=" 
	
SET NOCOUNT ON;	

SELECT DISTINCT COD_PRODUTO, 'S' VENDA 
INTO #TMP_VENDAS
FROM DW.DBO.BI_ANAL_TICKET WHERE COD_PRODUTO IN (SELECT COD_PRODUTO FROM 	BI.[DBO].[VW_CUSTOS_FIXOS_ATIVOS])
AND CONVERT(DATE,DATA) >= (`1`)

SELECT 	FIXOS.COD_PRODUTO	
		,+'['+ BI.DESCRICAO+']' DESCRICAO
		,NO_DEPARTAMENTO
		,NO_SECAO
		,FIXOS.COD_FORNECEDOR	
		,+'['+FORN.DESCRICAO+']' FORNECEDOR
		,FORA_LINHA
		,CAST(FIXOS.VLR_EMB_COMPRA	 AS DOUBLE PRECISION) VLR_EMB_COMPRA
		,CAST(FIXOS.QTD_EMB_COMPRA AS DOUBLE PRECISION) QTD_EMB_COMPRA
		,ISNULL(VENDA,'N') AS VENDA_2016
FROM BI.[DBO].[VW_CUSTOS_FIXOS_ATIVOS] FIXOS
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO BI
				ON BI.COD_PRODUTO = FIXOS.COD_PRODUTO
		LEFT JOIN  BI.DBO.BI_CAD_FORNECEDOR FORN
				ON FORN.COD_FORNECEDOR = FIXOS.COD_FORNECEDOR
		LEFT JOIN #TMP_VENDAS VENDA
				ON VENDA.COD_PRODUTO = FIXOS.COD_PRODUTO


DROP TABLE #TMP_VENDAS
		
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLDateTime@@DtaIni}];
	r
];

(*r3\[LeftArrow]getExcel[$DtaIni];*)


ClearAll@createXLSXReport;
createXLSXReport[]:=Module[{subject,body,tables,dates,intervalType,tempFile,destFile,r3,u,r6,newFile,resp},
	
	tempFile=FileNameJoin@{mrtFileDirectory[],"Trocas_e_Consignados.xlsx"};
	$destFile=FileNameJoin@{mrtFileDirectory[],"reports","Lista de Produtos Custo Fixo em "<>$now2<>".xlsx"};
	
	r3\[LeftArrow]getExcel[$DtaIni];

	Check[
		mrtUpdateExcelTemplate[
		 tempFile
		,$destFile
		,{ 
                <|"Sheet" -> "Fornecedores", "TableBase" -> "Forn" , "Data" -> r3["Data"]|>
		 }
		];
		,$destFile=$Failed
	];
	
	$destFile
]

createXLSXReport[];


getResumo[DtaIni_]:=Module[{sql,r,conn=marcheConn2[],tab},
sql=" 
	
SET NOCOUNT ON;	

SELECT DISTINCT COD_PRODUTO, 'S' VENDA 
INTO #TMP_VENDAS
FROM DW.DBO.BI_ANAL_TICKET WHERE COD_PRODUTO IN (SELECT COD_PRODUTO FROM 	BI.[DBO].[VW_CUSTOS_FIXOS_ATIVOS])
AND CONVERT(DATE,DATA) >= (`1`)

SELECT 	

COD_DEPARTAMENTO
,COUNT(DISTINCT FIXOS.COD_PRODUTO) QTD_ITENS
,SUM(CASE WHEN FORA_LINHA = 'S' THEN 1 ELSE 0 END) FORA_LINHA
,SUM(CASE WHEN FORA_LINHA = 'N' THEN 1 ELSE 0 END) LINHA
,SUM(CASE WHEN FIXOS.VLR_EMB_COMPRA	= 0  AND VENDA = 'S'   THEN 1 ELSE 0 END) VALOR_ZERO
,SUM(CASE WHEN ISNULL(VENDA,'N') = 'S'  THEN 1 ELSE 0 END) VENDA

INTO #TMP_RESUMO
FROM BI.[DBO].[VW_CUSTOS_FIXOS_ATIVOS] FIXOS
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO BI
				ON BI.COD_PRODUTO = FIXOS.COD_PRODUTO
		LEFT JOIN  BI.DBO.BI_CAD_FORNECEDOR FORN
				ON FORN.COD_FORNECEDOR = FIXOS.COD_FORNECEDOR
		LEFT JOIN #TMP_VENDAS VENDA
				ON VENDA.COD_PRODUTO = FIXOS.COD_PRODUTO
GROUP BY COD_DEPARTAMENTO


SELECT 
NO_DEPARTAMENTO	Dep	
,isnull(QTD_ITENS,0)	[Qtde Itens]
,isnull(LINHA,0)		[Em Linha]
,isnull(FORA_LINHA,0)	[Fora Linha]
,isnull(VALOR_ZERO,0)	[Qtde Custo Zero]
,isnull(VENDA,0)		[Venda 2016]	

 FROM 
		(
				SELECT DISTINCT
				COD_DEPARTAMENTO 
				,NO_DEPARTAMENTO 
				FROM BI.DBO.BI_CAD_PRODUTO
		) A 
		LEFT JOIN #TMP_RESUMO B
				ON B.COD_DEPARTAMENTO = A.COD_DEPARTAMENTO

ORDER BY A.COD_DEPARTAMENTO

DROP TABLE #TMP_VENDAS
DROP TABLE #TMP_RESUMO
		
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLDateTime@@DtaIni}];
	r
];

r1\[LeftArrow]getResumo[$DtaIni];


gridLojas[data_Symbol]:=Module[{grid,title,r,color2,stl,total},
	r\[LeftArrow]data;

	   total={"Total",Total@r[[All,"Qtde Itens"]],Total@r[[All,"Em Linha"]],Total@r[[All,"Fora Linha"]],Total@r[[All,"Qtde Custo Zero"]],Total@r[[All,"Venda 2016"]]};
       r["DataAll"]=Join[r["DataAll"],{total}];
	
	color1=Which[ 
	 #>0,Red
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

	grid=maReportGrid[r,"ColumnFormat"->{{"Qtde Custo Zero"}-> stl},"TotalRow"->True];
	title=Style[Row@{"Status Custo Fixo"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridLojas[r1];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{gridLojas[r1]}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];

rep=marcheTemplate[grid=Row@{"Status Custo Fixo ",$now},createReport[s],1200, $logo];
$fileName1="Status Custo Fixo "<>$now2<>".png";
Export["Status Custo Fixo "<>$now2<>".png",rep];


getResultado[DtaIni_]:=Module[{sql,r,conn=marcheConn2[],tab},
sql=" 
	
SET NOCOUNT ON;	

SELECT
COUNT(DISTINCT FIXOS.COD_PRODUTO) QTD_ITENS
FROM BI.[DBO].[VW_CUSTOS_FIXOS_ATIVOS] FIXOS
		
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	$Resultado=ToString@r["Data"][[1,1]]
];

r2\[LeftArrow]getResultado[$DtaIni];


marcheMail[
  		"[GE] Constam " <>$Resultado<> " produtos listados como Custo Fixo em " <>$now
  		,""
		  , $mailsGerencia
		  ,{$fileName1,$destFile}	
]


(*"diego.miller@marche.com.br"*)
