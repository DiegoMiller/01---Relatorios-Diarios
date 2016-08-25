(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["Marche.png"];
$lojas={12,13};
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[DatePlus[-1],{"Day","-","Month","-","Year"}];
$DateOntem=DateString[DatePlus[-1], {"Day", "/", "Month", "/", "Year"}];


getCONTROLTRANSF[codLoja_,CodAjuste_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 

SET NOCOUNT ON;

SELECT 
		COD_LOJA
		,CONVERT(VARCHAR,DTA_AJUSTE,103) AS DATA
		,AJUSTE.COD_PRODUTO*1 AS PLU
		,DESCRICAO
		,NO_DEPARTAMENTO as DEPARTAMENTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTDE_LAN\[CapitalCCedilla]ADA
		,SUM((QTD_AJUSTE*VAL_CUSTO_REP)*-1) AS CUSTO
INTO #TAB_AJUSTE
FROM TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK) LEFT JOIN[192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS CAD ON AJUSTE.COD_PRODUTO = CAD.COD_PRODUTO
WHERE 1=1
		AND COD_AJUSTE IN  (`2`)
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DTA_AJUSTE) = CONVERT(DATE,GETDATE()-1)
GROUP BY 
		COD_LOJA
		,DTA_AJUSTE
		,AJUSTE.COD_PRODUTO
		,DESCRICAO
		,NO_DEPARTAMENTO
	
SELECT * FROM #TAB_AJUSTE
			
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLArgument@@CodAjuste}];
	r
];
r\[LeftArrow]getCONTROLTRANSF[$lojas,153]


(*r["DataAll"]//mrtCopyTable2ClipBoard*)


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};


$fileName1="TRANSFERENCIAS PARA PRODUCAO CENTRAL.xlsx";
Export["TRANSFERENCIAS PARA PRODUCAO CENTRAL.xlsx",r["DataAll"]];		


getCONTROLTRANSFTOTAL[codLoja_,CodAjuste_]:=Module[{sql,s,conn=marcheConn[],tab},
	sql=" 

SET NOCOUNT ON;

SELECT 
		NO_DEPARTAMENTO AS DEPARTAMENTO
		,SUM((QTD_AJUSTE*VAL_CUSTO_REP)*-1) AS CUSTO
INTO #TAB_AJUSTE
FROM TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK) LEFT JOIN[192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS CAD ON AJUSTE.COD_PRODUTO = CAD.COD_PRODUTO
WHERE 1=1
		AND COD_AJUSTE IN  (`2`)
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DTA_AJUSTE) = CONVERT(DATE,GETDATE()-1)
GROUP BY 
		NO_DEPARTAMENTO

SELECT 
		DISTINCT (NO_DEPARTAMENTO) AS DEPARTAMENTO  
		,CONVERT(DECIMAL(10,2),(ISNULL(CUSTO,0))) AS CUSTO 
FROM [192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS CAD
LEFT JOIN #TAB_AJUSTE AS TAB_AJUSTE ON TAB_AJUSTE.DEPARTAMENTO = CAD.NO_DEPARTAMENTO
UNION
SELECT 'TOTAL' AS TOTAL, SUM(CUSTO) AS TOTAL  FROM #TAB_AJUSTE

";
	s\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLArgument@@CodAjuste}];
	s
];
j\[LeftArrow]getCONTROLTRANSFTOTAL[12,153]
p\[LeftArrow]getCONTROLTRANSFTOTAL[13,153]


p["DataAll"]//mrtCopyTable2ClipBoard


gridTOTAL[data_Symbol]:=Module[{grid,title,s,color2,stl},
	s\[LeftArrow]data;
	color2=Which[ 
	 #<=0,Darker@Green
	,True,Red
	]&;

    stl[val_]:=Style[maF["N,00"][val],Bold,color2@val];
	stl[Null]="0,00"; 

	grid=maReportGrid[s,"ColumnFormat"->{{"CUSTO"}-> stl},"TotalRow"->True];
	title=Style[Row@{"JAUAPERI"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridTOTAL[j];


gridTOTAL2[data_Symbol]:=Module[{grid,title,s,color2,stl},
	s\[LeftArrow]data;
	color2=Which[ 
	 #<=0,Darker@Green
	,True,Red
	]&;

    stl[val_]:=Style[maF["N,00"][val],Bold,color2@val];
	stl[Null]="0,00"; 

	grid=maReportGrid[s,"ColumnFormat"->{{"CUSTO"}-> stl},"TotalRow"->True];
	title=Style[Row@{"PAV\[CapitalATilde]O"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridTOTAL2[p];


createReport[data_Symbol]:=Module[{r},
	r\[LeftArrow]data;
	Grid[{{gridTOTAL[j],gridTOTAL2[p]}},Spacings->{4,4},Frame->Transparent]
]
createReport[r];


rep=marcheTemplate[grid=Row@{"Resumo de Transfer\[EHat]ncias para Produ\[CCedilla]\[ATilde]o Central em ",$DateOntem},createReport[r],1200, $logo];

$fileName2="Resumo de Transfer\[EHat]ncias para Produ\[CCedilla]\[ATilde]o Central.png";
Export["Resumo de Transfer\[EHat]ncias para Produ\[CCedilla]\[ATilde]o Central.png",rep];


marcheMail[
  		"[GE] Transfer\[EHat]ncias para Produ\[CCedilla]\[ATilde]o Central em " <>$DateOntem
  		,"Resumo de Transfer\[EHat]ncias para Produ\[CCedilla]\[ATilde]o Central\n\nLembrando que:\n\n- Produtos utilizados pela Produ\[CCedilla]\[ATilde]o Central ser\[ATilde]o comprados no mesmo PLU da loja, sempre que a loja tiver o produto para revenda e fazer a transfer\[EHat]ncia no PLU de compra;\n- Usaremos para compra e transfer\[EHat]ncia de PLU insumo apenas os produtos n\[ATilde]o revendidos."
  		,$mailsGerencia
		  ,{$fileName2,$fileName1}
]
