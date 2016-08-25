(* ::Package:: *)

(* ::Section:: *)
(*DNV*)


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


$lojas={1,2,3,6,7,9,12,13,17,18,20,21,22,23,24,25,27,29,30,31};
$deParaLoja=maGetDeParaLojas[];
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$mails=marcheGetMails[];
$cargosLoja={"GERENTE","COORD_PERECIVEL","COORD_RELACIONAMENTO","COORD_NAO_PERECIVEL","REGIONAL","COORD_PREVENCAO","FISCAL_PREVENCAO"};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];


getDNV[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 

SET NOCOUNT ON;

SELECT  DISTINCT
		COD_LOJA
		,COD_PRODUTO
		,MAX(CONVERT(DATE,DATA)) AS LAST_DATE 
INTO  #TMP_VENDA
FROM DW.DBO.BI_ANAL_TICKET  WITH (NOLOCK) 
WHERE 1=1 
		AND CONVERT(DATE,DATA) >= GETDATE()-4
		AND CONVERT(DATE,DATA) < GETDATE()
		AND COD_LOJA IN (`1`)

GROUP BY 
		COD_LOJA
		,COD_PRODUTO


SELECT * 
INTO #TMP_VENDA_3_DIAS
FROM #TMP_VENDA

UNION

SELECT COD_LOJA
		,ITEMID_PAI*1 ITEMID_PAI
		,LAST_DATE
FROM #TMP_VENDA VENDA 
		INNER JOIN [AX2009_INTEGRACAO].[DBO].[TAB_PRODUTO_BOM] BOM
				ON BOM.COD_PRODUTO = VENDA.COD_PRODUTO


SELECT  TOP 10 
		NO_LOJA AS LOJA
		,EP.COD_PRODUTO AS PLU
		,DESCRICAO
		,CLASSIF_PRODUTO AS ABC
		,NO_DEPARTAMENTO AS DEPARTAMENTO
		,NO_SECAO AS SE\[CapitalCCedilla]\[CapitalATilde]O
		,EP.QTD_ESTOQUE as ESTOQUE
 FROM DW.DBO.ESTOQUE AS EP WITH (NOLOCK) 
		LEFT JOIN #TMP_VENDA_3_DIAS AS VP
				ON (VP.COD_PRODUTO = EP.COD_PRODUTO AND VP.COD_LOJA = EP.COD_LOJA)
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO AS CP WITH (NOLOCK) 
				ON (CP.COD_PRODUTO	= EP.COD_PRODUTO)
		LEFT JOIN [BI].[DBO].[BI_CAD_LOJA2] as LJ with (NOLOCK)  
				ON (LJ.COD_LOJA = EP.COD_LOJA)
WHERE 1=1 
		AND CONVERT(DATE,EP.DATA) = CONVERT(DATE,GETDATE())
		AND VP.COD_PRODUTO IS NULL
		AND EP.COD_PRODUTO NOT IN (29537,28936,1027148)
		AND EP.COD_PRODUTO NOT IN (SELECT DISTINCT COD_PRODUTO FROM [BI].[DBO].CADASTRO_CAD_PRODUTO_METADADOS WHERE 1=1 AND COD_METADADO = 42)
	    --AND COD_DEPARTAMENTO NOT IN (15,7,20,17,99,16,21)
		AND COD_DEPARTAMENTO NOT IN (19,5,15,7,20,17,99,16,12,21)
		AND EP.COD_LOJA IN (`1`)
ORDER BY
		EP.QTD_ESTOQUE DESC

DROP TABLE #TMP_VENDA_3_DIAS
															
";
r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r["DateInterval"]={dtIni1,dtFim1};
	r["CodLoja"]=codLoja;
	r["NoLoja"]=codLoja/.$deParaLoja;
	r["FileName"]=ToString@Row@{"DNV ",r["NoLoja"],".png"};
	r
]
r[99]=getDNV[$lojas]


(*ClearAll@r*)


i=0;Dynamic@i
Scan[(i++;r[#]=getDNV[#])&,$lojas]


gridDNV[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
				 #>0,Darker@Green
			,True,Red
	
	]&;

	stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="";

	grid=maReportGrid[r,"ColumnFormat"->{{"ESTOQUE"}-> stl},"TotalRow"->False];
	title=Style[Row@{"DNV ", \!\(\*
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

$fileName=ToString@Row@{"Resumo DNV.png"};
Export["Resumo DNV.png",gridDNV[r[99]]];


createFile[codLoja_Integer]:=Module[{},
	Export[r[codLoja]["FileName"],gridDNV[r[codLoja]]]
]


i=0;Dynamic@i
Scan[(i++;createFile[#])&,$lojas[[]]]


ClearAll@sendMails
sendMails[codLoja_Integer,mail_:Automatic]:=Module[{mails},

	mails=If[mail===Automatic,Flatten@$mails[codLoja,$cargosLoja],mail];

	marcheMail[
  		ToString@Row@{"Top 10 DNV ",codLoja/.$deParaLoja,"|",IntegerString[codLoja,10,2]," ",$now}
  		,"Relat\[OAcute]rio de DNV (Dias de n\[ATilde]o Venda)

Maiores estoques sem venda nos \[UAcute]ltimos 3 dias

Procedimentos que devem ser adotados pelas lojas:
- Verificar exposi\[CCedilla]\[ATilde]o;
- Verificar precifica\[CCedilla]\[ATilde]o;
- Verificar estoque (Caso exista diferen\[CCedilla]a entre o sistema e o f\[IAcute]sico informar o saldo correto atrav\[EAcute]s da Ruptura no Delph).


Para mudan\[CCedilla]a/corre\[CCedilla]\[ATilde]o de emails favor alterar
http://goo.gl/qWZu6x"
  		,mails
  		,{r[codLoja]["FileName"]}
  ]
]
(*sendMails[2,"diego.miller@marche.com.br"]*)


i=0;Dynamic@i
Scan[(i++;sendMails[#])&,$lojas[[]]]


marcheMail[
  		"[GE] Top 10 DNV por Loja " <>$now
  		,"Relat\[OAcute]rio de DNV (Dias de n\[ATilde]o Venda)

Maiores estoques sem venda nos \[UAcute]ltimos 3 dias

Procedimentos que devem ser adotados pelas lojas:
- Verificar exposi\[CCedilla]\[ATilde]o;
- Verificar precifica\[CCedilla]\[ATilde]o;
- Verificar estoque (Caso exista diferen\[CCedilla]a entre o sistema e o f\[IAcute]sico informar o saldo correto atrav\[EAcute]s da Ruptura no Delph)."
  		,$mailsGerencia
  		,{$fileName,r[1]["FileName"],r[2]["FileName"],r[3]["FileName"],r[6]["FileName"],r[7]["FileName"],r[9]["FileName"],r[12]["FileName"],r[13]["FileName"],r[17]["FileName"],r[18]["FileName"],r[20]["FileName"],r[21]["FileName"],r[22]["FileName"],r[23]["FileName"],r[24]["FileName"],r[25]["FileName"],r[27]["FileName"],r[29]["FileName"],r[30]["FileName"],r[31]["FileName"]}
  ]


(*(* Teste *)*)
(*i=0;Dynamic@i*)
(*Scan[(i++;sendMails[#,"diego.miller@marche.com.br"])&,$lojas[[;;3]]]*)
