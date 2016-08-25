(* ::Package:: *)

(* ::Section:: *)
(*ESTOQUE DIA FRIOS*)


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
SELECT 					
		NO_LOJA													AS Loja
		,CONVERT(VARCHAR,DATA,103)								 AS Data
		,CONVERT(TIME(0),DTA_GRAVACAO)								AS Horario
		,PM.COD_PRODUTO											AS Plu
		,DESCRICAO												 AS Descricao
		,NO_DEPARTAMENTO										   AS Departamento	
		,CLASSIF_PRODUTO                                           AS ABC
		,QTD_ESTOQUE											  AS 'Qtde Estoque'
			
		FROM DW.DBO.ESTOQUE AS PM
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO AS CAD
		ON PM.COD_PRODUTO = CAD.COD_PRODUTO
		LEFT JOIN [BI].[DBO].[BI_CAD_LOJA2] AS LJ WITH (NOLOCK) 
		ON LJ.COD_LOJA = PM.COD_LOJA

		WHERE 1=1	

		AND PM.DATA = CONVERT(DATE,GETDATE())
		AND PM.COD_LOJA IN (`1`)
		AND PM.COD_PRODUTO IN (70485,42819,87773)

";
r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r["DateInterval"]={dtIni1,dtFim1};
	r["CodLoja"]=codLoja;
	r["NoLoja"]=codLoja/.$deParaLoja;
	r["FileName"]=ToString@Row@{"Estoque Frios ",r["NoLoja"]," ",$now2,".png"};
	r
]
r[99]=getDNV[$lojas];


i=0;Dynamic@i
Scan[(i++;r[#]=getDNV[#])&,$lojas]


gridDNV[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
			#<0.04,Darker@Green
			,#<0.06,Orange,
			,True,Red
	]&;

	stl[val_]:=Style[maF["%"][val],Bold,color2@val];
	stl[Null]="";

	grid=maReportGrid[r,"ColumnFormat"->{{"Qtde Estoque"}-> "N,00"},"TotalRow"->False];
	title=Style[Row@{"Estoque Frios em ", \!\(\*
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

$fileName=ToString@Row@{"Resumo - Estoque Frios em "<>$now2<>".png"};
Export["Resumo - Estoque Frios em "<>$now2<>".png",gridDNV[r[99]]];


createFile[codLoja_Integer]:=Module[{},
	Export[r[codLoja]["FileName"],gridDNV[r[codLoja]]]
]


i=0;Dynamic@i
Scan[(i++;createFile[#])&,$lojas[[]]]


marcheMail[
  		"Estoque Frios em " <>$now
  		,"Relat\[OAcute]rio de Estoque PEITO PERU SADIA KG e PRESUNTO COZIDO TRADS/CAPA SADIA F KG

Procedimentos que devem ser adotados pelas lojas:
- Verificar estoque (Caso exista diferen\[CCedilla]a entre o sistema e o f\[IAcute]sico informar o saldo correto atrav\[EAcute]s da Ruptura no Delph).

Obs. Sempre somar o estoque do PEITO DE PERU COM CASCA E SEM CASCA.
"
  		,$mailsGerencia(*{"cleberton.silva@marche.com.br","diego.miller@marche.com.br"}*)
  		,{r[1]["FileName"],r[2]["FileName"],r[3]["FileName"],r[6]["FileName"],r[7]["FileName"],r[9]["FileName"],r[12]["FileName"],r[13]["FileName"],r[17]["FileName"],r[18]["FileName"],r[20]["FileName"],r[21]["FileName"],r[22]["FileName"],r[23]["FileName"],r[24]["FileName"],r[25]["FileName"],r[27]["FileName"],r[29]["FileName"],r[30]["FileName"],r[31]["FileName"]}
  ]
