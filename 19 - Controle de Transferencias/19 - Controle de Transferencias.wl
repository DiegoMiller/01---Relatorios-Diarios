(* ::Package:: *)

(* ::Section:: *)
(*TRANSFERENCIAS REALIZADAS*)


Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


maGetDeParaLojas[]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql="
		SELECT  [COD_LOJA]
			  ,[NO_LOJA]
		FROM [BI].[dbo].[BI_CAD_LOJA2]
	";
	r\[LeftArrow]mrtSQLDataObject[conn,sql];
	Rule@@@r["Data"]
]
maGetDeParaLojas[];


$lojas={1,2,3,6,7,9,12,13,17,18,20,21,22,23,24,25,27,29,30,31,33};
$deParaLoja=maGetDeParaLojas[];
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$mails=marcheGetMails[];
$cargosLoja={"GERENTE","COORD_PERECIVEL","COORD_RELACIONAMENTO","COORD_NAO_PERECIVEL","REGIONAL","COORD_PREVENCAO","FISCAL_PREVENCAO"};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$dtIni=DateString[DatePlus[-7],{"Year","Month","Day"}];
$dtFim=DateString[DatePlus[-1],{"Year","Month","Day"}];
$Date=ToString@Row[DateString[#,{"Day","/","Month","/","YearShort"}]&/@{$dtIni,$dtFim}," \[AGrave] "];
$Date2=ToString@Row[DateString[#,{"Day","-","Month","-","YearShort"}]&/@{$dtIni,$dtFim}," \[AGrave] "];
SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};


getTRANSFS[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 

		SET NOCOUNT ON;
SELECT  
		NO_LOJA AS Loja
		,NO_REGIONAL AS Regional
		,DES_AJUSTE AS Tipo 
		,COUNT(CAST(QTDE AS DOUBLE PRECISION)) AS [Qtde Itens]
		,SUM(QTDE*VAL_CUSTO_REP) AS Custo
FROM [INTRANET].[DBO].[TAB_TRANSFINTERNAS] AS T INNER JOIN [INTRANET].[DBO].[TAB_TRANSFINTERNAS_PROD] AS P ON T.ID =P.IDTRANSF
LEFT JOIN ZEUS_RTG.DBO.TAB_CODIGO_BARRA AS ZEUS_BARRA (NOLOCK) ON CONVERT(NUMERIC,P.COD_EAN) = CONVERT(NUMERIC,ZEUS_BARRA.COD_EAN)
LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS TP ON TP.COD_PRODUTO = ZEUS_BARRA.COD_PRODUTO 
LEFT JOIN TAB_TIPO_AJUSTE AS AJUSTE ON AJUSTE.COD_AJUSTE = T.TIPO
LEFT JOIN TAB_PRODUTO_LOJA AS CUSTO ON CUSTO.COD_LOJA = T.COD_LOJA AND CUSTO.COD_PRODUTO = ZEUS_BARRA.COD_PRODUTO 
LEFT JOIN TAB_USUARIO AS USUARIO ON USUARIO.COD_USUARIO = T.APROVADOR
LEFT JOIN [192.168.0.13].[BI].[DBO].[BI_CAD_LOJA2] AS LJ ON LJ.COD_LOJA = T.[COD_LOJA]
WHERE 1=1

AND T.STATUS IN (7)
AND T.COD_LOJA IN (`1`)
AND CONVERT(DATE,DATA) BETWEEN CONVERT(DATE,GETDATE()-7) AND CONVERT(DATE,GETDATE()-1)
AND TIPO IN (9,70,71,72,73,74,75,89,94,108,109,110,115,116,117,118,125,126,127,129,153,156,213,214,216,217,224,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,269,287,289,290,291)

GROUP BY
		NO_LOJA
		,NO_REGIONAL
		,DES_AJUSTE

";
r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	(*r["DateInterval"]={dtIni,dtFim};*)
	r["CodLoja"]=codLoja;
	r["NoLoja"]=codLoja/.$deParaLoja;
	r["FileName"]=ToString@Row@{"Transfer\[EHat]ncias ",r["NoLoja"]," ",$now2,".png"};
	r
]
r[99]=getTRANSFS[1]


i=0;Dynamic@i
Scan[(i++;r[#]=getTRANSFS[#])&,$lojas]


$Date=ToString@Row[DateString[#,{"Day","/","Month","/","YearShort"}]&/@{$dtIni,$dtFim}," \[AGrave] "];

gridTRANSF[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
			#<0.04,Darker@Green
			,#<0.06,Orange,
			,True,Red
	]&;

	stl[val_]:=Style[maF["%"][val],Bold,color2@val];
	stl[Null]="";

	grid=maReportGrid[r,"ColumnFormat"->{{"Custo"}-> "N,00"},"TotalRow"->False];
	title=Style[Row@{"Transfer\[EHat]ncias efetivadas em ", $Date},maTitleFormatOptions];
	(maReportFrame[title,grid])
]
gridTRANSF[r[1]]


createFile[codLoja_Integer]:=Module[{},
	Export[r[codLoja]["FileName"],gridTRANSF[r[codLoja]]]
]


i=0;Dynamic@i
Scan[(i++;createFile[#])&,$lojas[[]]]


getTRANSFS2[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 

		SET NOCOUNT ON;

SELECT  
		 CONVERT(DATE,[DATA]) AS DATA 
		,T.[COD_LOJA]
		,NO_LOJA
		,NO_REGIONAL
		,ZEUS_BARRA.COD_PRODUTO 
		,DESCRICAO
		,[TIPO]
		,DES_AJUSTE
		,[OBS]
		,convert(varchar,[RESPONSAVEL]) AS[RESPONSAVEL]
		,VAL_CUSTO_REP
		,CAST (QTDE AS DOUBLE PRECISION) AS QTDE
		,QTDE*VAL_CUSTO_REP as CUSTO
		,convert(varchar,Aprovador) AS Aprovador
FROM [INTRANET].[DBO].[TAB_TRANSFINTERNAS] AS T INNER JOIN [INTRANET].[DBO].[TAB_TRANSFINTERNAS_PROD] AS P ON T.ID =P.IDTRANSF
LEFT JOIN ZEUS_RTG.DBO.TAB_CODIGO_BARRA AS ZEUS_BARRA (NOLOCK) ON CONVERT(NUMERIC,P.COD_EAN) = CONVERT(NUMERIC,ZEUS_BARRA.COD_EAN)
LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS TP ON TP.COD_PRODUTO = ZEUS_BARRA.COD_PRODUTO 
LEFT JOIN TAB_TIPO_AJUSTE AS AJUSTE ON AJUSTE.COD_AJUSTE = T.TIPO
LEFT JOIN TAB_PRODUTO_LOJA AS CUSTO ON CUSTO.COD_LOJA = T.COD_LOJA AND CUSTO.COD_PRODUTO = ZEUS_BARRA.COD_PRODUTO 
LEFT JOIN TAB_USUARIO AS USUARIO ON USUARIO.COD_USUARIO = T.Aprovador
LEFT JOIN [192.168.0.13].[BI].[DBO].[BI_CAD_LOJA2] AS LJ ON LJ.COD_LOJA = T.[COD_LOJA]
WHERE 1=1

AND T.STATUS IN (7)
AND T.COD_LOJA IN (`1`)
AND CONVERT(DATE,DATA) BETWEEN CONVERT(DATE,GETDATE()-7) AND CONVERT(DATE,GETDATE()-1)
AND TIPO IN (9,70,71,72,73,74,75,89,94,108,109,110,115,116,117,118,125,126,127,129,153,156,213,214,216,217,224,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,269,287,289,290,291)

";
r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	(*r["DateInterval"]={dtIni,dtFim};*)
	r["CodLoja"]=codLoja;
	r["NoLoja"]=codLoja/.$deParaLoja;
	r["FileName"]=ToString@Row@{"Transfer\[EHat]ncias ",r["NoLoja"]," ",$now2,".png"};
	r
]
r[99]=getTRANSFS2[$lojas]


$fileNameTRANSF="Relacao de produtos transferidos em "<>$Date2<>".xlsx";
Export["Relacao de produtos transferidos em "<>$Date2<>".xlsx",r[99]["DataAll"]];


marcheMail[
  		"[GE] Transfer\[EHat]ncias efetivadas em " <>$Date
  		,"Transfer\[EHat]ncias efetivadas em " <>$Date
  		,$mailsGerencia
  		,{r[1]["FileName"],r[2]["FileName"],r[3]["FileName"],r[6]["FileName"],r[7]["FileName"],r[9]["FileName"],r[12]["FileName"],r[13]["FileName"],r[17]["FileName"],r[18]["FileName"],r[20]["FileName"],r[21]["FileName"],r[22]["FileName"],r[23]["FileName"],r[24]["FileName"],r[25]["FileName"],r[27]["FileName"],r[29]["FileName"],r[30]["FileName"],r[31]["FileName"],$fileNameTRANSF}
  ]
