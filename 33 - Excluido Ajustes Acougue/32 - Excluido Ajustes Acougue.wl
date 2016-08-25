(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$lojas={121};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];


getRegistros[codLoja_]:=Module[{sql,r,conn=marcheConn3[],tab},
sql=" 
	
SET NOCOUNT ON;	

SELECT count(*) REGISTROS FROM 
[192.168.0.6].ZEUS_RTG.DBO.TAB_AJUSTE_ESTOQUE 
WHERE  1=1
		AND COD_LOJA NOT IN (21)
		AND COD_AJUSTE IN (3,4,53,6)
		AND CONVERT(DATE,DTA_AJUSTE) <  CONVERT(DATE,'2016-05-03')

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql];
	
	$Resultado=ToString@r["Data"][[1,1]]

];
r\[LeftArrow]getRegistros[$lojas]


$Ajuste=Import["Ajustes.txt"];
$pMail=Export["Ajuste.txt",$Ajuste];


marcheMail[
  		"[GE] Existem " <>$Resultado "registros do a\[CCedilla]ougue para serem excluidos em " <>$now
  		,"Ajustes para serem excluidos"
		  ,"diego.miller@marche.com.br"
		  ,$pMail
]
