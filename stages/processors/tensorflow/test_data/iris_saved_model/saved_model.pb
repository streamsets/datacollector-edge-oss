??
??
:
Add
x"T
y"T
z"T"
Ttype:
2	
?
ArgMax

input"T
	dimension"Tidx
output"output_type" 
Ttype:
2	"
Tidxtype0:
2	"
output_typetype0	:
2	
?
AsString

input"T

output"
Ttype:
	2	
"
	precisionint?????????"

scientificbool( "
shortestbool( "
widthint?????????"
fillstring 
x
Assign
ref"T?

value"T

output_ref"T?"	
Ttype"
validate_shapebool("
use_lockingbool(?
~
BiasAdd

value"T	
bias"T
output"T" 
Ttype:
2	"-
data_formatstringNHWC:
NHWCNCHW
8
Cast	
x"SrcT	
y"DstT"
SrcTtype"
DstTtype
h
ConcatV2
values"T*N
axis"Tidx
output"T"
Nint(0"	
Ttype"
Tidxtype0:
2	
8
Const
output"dtype"
valuetensor"
dtypetype
B
Equal
x"T
y"T
z
"
Ttype:
2	
?
W

ExpandDims

input"T
dim"Tdim
output"T"	
Ttype"
Tdimtype0:
2	
V
HistogramSummary
tag
values"T
summary"
Ttype0:
2	
.
Identity

input"T
output"T"	
Ttype
p
MatMul
a"T
b"T
product"T"
transpose_abool( "
transpose_bbool( "
Ttype:
	2
?
Mean

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( " 
Ttype:
2	"
Tidxtype0:
2	
e
MergeV2Checkpoints
checkpoint_prefixes
destination_prefix"
delete_old_dirsbool(?
=
Mul
x"T
y"T
z"T"
Ttype:
2	?

NoOp
M
Pack
values"T*N
output"T"
Nint(0"	
Ttype"
axisint 
C
Placeholder
output"dtype"
dtypetype"
shapeshape:
~
RandomUniform

shape"T
output"dtype"
seedint "
seed2int "
dtypetype:
2"
Ttype:
2	?
a
Range
start"Tidx
limit"Tidx
delta"Tidx
output"Tidx"
Tidxtype0:	
2	
D
Relu
features"T
activations"T"
Ttype:
2	
[
Reshape
tensor"T
shape"Tshape
output"T"	
Ttype"
Tshapetype0:
2	
o
	RestoreV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0?
l
SaveV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0?
P
ScalarSummary
tags
values"T
summary"
Ttype:
2	
P
Shape

input"T
output"out_type"	
Ttype"
out_typetype0:
2	
H
ShardedFilename
basename	
shard

num_shards
filename
9
Softmax
logits"T
softmax"T"
Ttype:
2
?
StridedSlice

input"T
begin"Index
end"Index
strides"Index
output"T"	
Ttype"
Indextype:
2	"

begin_maskint "
end_maskint "
ellipsis_maskint "
new_axis_maskint "
shrink_axis_maskint 
N

StringJoin
inputs*N

output"
Nint(0"
	separatorstring 
:
Sub
x"T
y"T
z"T"
Ttype:
2	
c
Tile

input"T
	multiples"
Tmultiples
output"T"	
Ttype"

Tmultiplestype0:
2	
s

VariableV2
ref"dtype?"
shapeshape"
dtypetype"
	containerstring "
shared_namestring ?"serve*1.8.02v1.8.0-0-g93bc2e2072??
?
!save_1/RestoreV2/shape_and_slicesConst"/device:CPU:0*
dtype0*
_output_shapes
:	*w
valuenBl	B10 0,10B4 10 0,4:0,10B20 0,20B10 20 0,10:0,20B10 0,10B20 10 0,20:0,10B3 0,3B10 3 0,10:0,3B 
?
save_1/RestoreV2/tensor_namesConst"/device:CPU:0*?
value?B?	Bdnn/hiddenlayer_0/biasBdnn/hiddenlayer_0/kernelBdnn/hiddenlayer_1/biasBdnn/hiddenlayer_1/kernelBdnn/hiddenlayer_2/biasBdnn/hiddenlayer_2/kernelBdnn/logits/biasBdnn/logits/kernelBglobal_step*
dtype0*
_output_shapes
:	
?
save_1/SaveV2/shape_and_slicesConst"/device:CPU:0*w
valuenBl	B10 0,10B4 10 0,4:0,10B20 0,20B10 20 0,10:0,20B10 0,10B20 10 0,20:0,10B3 0,3B10 3 0,10:0,3B *
dtype0*
_output_shapes
:	
?
save_1/SaveV2/tensor_namesConst"/device:CPU:0*?
value?B?	Bdnn/hiddenlayer_0/biasBdnn/hiddenlayer_0/kernelBdnn/hiddenlayer_1/biasBdnn/hiddenlayer_1/kernelBdnn/hiddenlayer_2/biasBdnn/hiddenlayer_2/kernelBdnn/logits/biasBdnn/logits/kernelBglobal_step*
dtype0*
_output_shapes
:	
m
save_1/ShardedFilename/shardConst"/device:CPU:0*
value	B : *
dtype0*
_output_shapes
: 
S
save_1/num_shardsConst*
value	B :*
dtype0*
_output_shapes
: 
?
save_1/StringJoin/inputs_1Const*<
value3B1 B+_temp_31a69a0819ea4f878ad68e999a3ee4b3/part*
dtype0*
_output_shapes
: 
R
save_1/ConstConst*
valueB Bmodel*
dtype0*
_output_shapes
: 
?
save_1/RestoreV2	RestoreV2save_1/Constsave_1/RestoreV2/tensor_names!save_1/RestoreV2/shape_and_slices"/device:CPU:0*X
_output_shapesF
D:
:
::
:
:
::
:*
dtypes
2		
{
save_1/StringJoin
StringJoinsave_1/Constsave_1/StringJoin/inputs_1*
N*
_output_shapes
: *
	separator 
?
save_1/ShardedFilenameShardedFilenamesave_1/StringJoinsave_1/ShardedFilename/shardsave_1/num_shards"/device:CPU:0*
_output_shapes
: 

init_1NoOp

init_all_tablesNoOp

initNoOp
4

group_depsNoOp^init^init_1^init_all_tables
?
save/RestoreV2/shape_and_slicesConst"/device:CPU:0*
_output_shapes
:	*w
valuenBl	B10 0,10B4 10 0,4:0,10B20 0,20B10 20 0,10:0,20B10 0,10B20 10 0,20:0,10B3 0,3B10 3 0,10:0,3B *
dtype0
?
save/RestoreV2/tensor_namesConst"/device:CPU:0*?
value?B?	Bdnn/hiddenlayer_0/biasBdnn/hiddenlayer_0/kernelBdnn/hiddenlayer_1/biasBdnn/hiddenlayer_1/kernelBdnn/hiddenlayer_2/biasBdnn/hiddenlayer_2/kernelBdnn/logits/biasBdnn/logits/kernelBglobal_step*
dtype0*
_output_shapes
:	
?
save/SaveV2/shape_and_slicesConst"/device:CPU:0*w
valuenBl	B10 0,10B4 10 0,4:0,10B20 0,20B10 20 0,10:0,20B10 0,10B20 10 0,20:0,10B3 0,3B10 3 0,10:0,3B *
dtype0*
_output_shapes
:	
?
save/SaveV2/tensor_namesConst"/device:CPU:0*?
value?B?	Bdnn/hiddenlayer_0/biasBdnn/hiddenlayer_0/kernelBdnn/hiddenlayer_1/biasBdnn/hiddenlayer_1/kernelBdnn/hiddenlayer_2/biasBdnn/hiddenlayer_2/kernelBdnn/logits/biasBdnn/logits/kernelBglobal_step*
dtype0*
_output_shapes
:	
k
save/ShardedFilename/shardConst"/device:CPU:0*
value	B : *
dtype0*
_output_shapes
: 
Q
save/num_shardsConst*
value	B :*
dtype0*
_output_shapes
: 
?
save/StringJoin/inputs_1Const*<
value3B1 B+_temp_bb37a12246dd47a1ae0698b3e092bb9a/part*
dtype0*
_output_shapes
: 
P

save/ConstConst*
valueB Bmodel*
dtype0*
_output_shapes
: 
?
save/RestoreV2	RestoreV2
save/Constsave/RestoreV2/tensor_namessave/RestoreV2/shape_and_slices"/device:CPU:0*
dtypes
2		*X
_output_shapesF
D:
:
::
:
:
::
:
u
save/StringJoin
StringJoin
save/Constsave/StringJoin/inputs_1*
N*
_output_shapes
: *
	separator 
?
save/ShardedFilenameShardedFilenamesave/StringJoinsave/ShardedFilename/shardsave/num_shards"/device:CPU:0*
_output_shapes
: 
[
dnn/head/Tile/multiples/1Const*
_output_shapes
: *
value	B :*
dtype0
Y
dnn/head/ExpandDims/dimConst*
value	B : *
dtype0*
_output_shapes
: 
V
dnn/head/range/deltaConst*
value	B :*
dtype0*
_output_shapes
: 
V
dnn/head/range/limitConst*
value	B :*
dtype0*
_output_shapes
: 
V
dnn/head/range/startConst*
_output_shapes
: *
value	B : *
dtype0
?
dnn/head/rangeRangednn/head/range/startdnn/head/range/limitdnn/head/range/delta*
_output_shapes
:*

Tidx0
?
dnn/head/AsStringAsStringdnn/head/range*

fill *

scientific( *
width?????????*
_output_shapes
:*
shortest( *
	precision?????????*
T0
?
dnn/head/ExpandDims
ExpandDimsdnn/head/AsStringdnn/head/ExpandDims/dim*
T0*
_output_shapes

:*

Tdim0
h
dnn/head/strided_slice/stack_2Const*
valueB:*
dtype0*
_output_shapes
:
h
dnn/head/strided_slice/stack_1Const*
valueB:*
dtype0*
_output_shapes
:
f
dnn/head/strided_slice/stackConst*
valueB: *
dtype0*
_output_shapes
:
n
#dnn/head/predictions/ExpandDims/dimConst*
valueB :
?????????*
dtype0*
_output_shapes
: 
s
(dnn/head/predictions/class_ids/dimensionConst*
valueB :
?????????*
dtype0*
_output_shapes
: 
L
Ddnn/head/logits/assert_rank_at_least/static_checks_determined_all_okNoOp
[
Sdnn/head/logits/assert_rank_at_least/assert_type/statically_determined_correct_typeNoOp
k
)dnn/head/logits/assert_rank_at_least/rankConst*
_output_shapes
: *
value	B :*
dtype0
w
dnn/dnn/logits/activation/tagConst**
value!B Bdnn/dnn/logits/activation*
dtype0*
_output_shapes
: 
?
+dnn/dnn/logits/fraction_of_zero_values/tagsConst*7
value.B, B&dnn/dnn/logits/fraction_of_zero_values*
dtype0*
_output_shapes
: 
j
dnn/zero_fraction_3/ConstConst*
valueB"       *
dtype0*
_output_shapes
:
]
dnn/zero_fraction_3/zeroConst*
valueB
 *    *
dtype0*
_output_shapes
: 
?
dnn/logits/bias/part_0
VariableV2*
shared_name *)
_class
loc:@dnn/logits/bias/part_0*
	container *
shape:*
dtype0*
_output_shapes
:
?
save_1/Assign_6Assigndnn/logits/bias/part_0save_1/RestoreV2:6*
_output_shapes
:*
use_locking(*
T0*)
_class
loc:@dnn/logits/bias/part_0*
validate_shape(
?
save/Assign_6Assigndnn/logits/bias/part_0save/RestoreV2:6*
use_locking(*
T0*)
_class
loc:@dnn/logits/bias/part_0*
validate_shape(*
_output_shapes
:
?
dnn/logits/bias/part_0/readIdentitydnn/logits/bias/part_0*)
_class
loc:@dnn/logits/bias/part_0*
_output_shapes
:*
T0
]
dnn/logits/biasIdentitydnn/logits/bias/part_0/read*
T0*
_output_shapes
:
?
(dnn/logits/bias/part_0/Initializer/zerosConst*)
_class
loc:@dnn/logits/bias/part_0*
valueB*    *
dtype0*
_output_shapes
:
?
dnn/logits/bias/part_0/AssignAssigndnn/logits/bias/part_0(dnn/logits/bias/part_0/Initializer/zeros*
use_locking(*
T0*)
_class
loc:@dnn/logits/bias/part_0*
validate_shape(*
_output_shapes
:
?
dnn/logits/kernel/part_0
VariableV2*
dtype0*
_output_shapes

:
*
shared_name *+
_class!
loc:@dnn/logits/kernel/part_0*
	container *
shape
:

?
save_1/Assign_7Assigndnn/logits/kernel/part_0save_1/RestoreV2:7*
use_locking(*
T0*+
_class!
loc:@dnn/logits/kernel/part_0*
validate_shape(*
_output_shapes

:

?
save/Assign_7Assigndnn/logits/kernel/part_0save/RestoreV2:7*
use_locking(*
T0*+
_class!
loc:@dnn/logits/kernel/part_0*
validate_shape(*
_output_shapes

:

?
dnn/logits/kernel/part_0/readIdentitydnn/logits/kernel/part_0*+
_class!
loc:@dnn/logits/kernel/part_0*
_output_shapes

:
*
T0
e
dnn/logits/kernelIdentitydnn/logits/kernel/part_0/read*
T0*
_output_shapes

:

?
7dnn/logits/kernel/part_0/Initializer/random_uniform/maxConst*
_output_shapes
: *+
_class!
loc:@dnn/logits/kernel/part_0*
valueB
 *??-?*
dtype0
?
7dnn/logits/kernel/part_0/Initializer/random_uniform/minConst*+
_class!
loc:@dnn/logits/kernel/part_0*
valueB
 *??-?*
dtype0*
_output_shapes
: 
?
7dnn/logits/kernel/part_0/Initializer/random_uniform/subSub7dnn/logits/kernel/part_0/Initializer/random_uniform/max7dnn/logits/kernel/part_0/Initializer/random_uniform/min*
T0*+
_class!
loc:@dnn/logits/kernel/part_0*
_output_shapes
: 
?
9dnn/logits/kernel/part_0/Initializer/random_uniform/shapeConst*+
_class!
loc:@dnn/logits/kernel/part_0*
valueB"
      *
dtype0*
_output_shapes
:
?
Adnn/logits/kernel/part_0/Initializer/random_uniform/RandomUniformRandomUniform9dnn/logits/kernel/part_0/Initializer/random_uniform/shape*
dtype0*
_output_shapes

:
*

seed *
T0*+
_class!
loc:@dnn/logits/kernel/part_0*
seed2 
?
7dnn/logits/kernel/part_0/Initializer/random_uniform/mulMulAdnn/logits/kernel/part_0/Initializer/random_uniform/RandomUniform7dnn/logits/kernel/part_0/Initializer/random_uniform/sub*
T0*+
_class!
loc:@dnn/logits/kernel/part_0*
_output_shapes

:

?
3dnn/logits/kernel/part_0/Initializer/random_uniformAdd7dnn/logits/kernel/part_0/Initializer/random_uniform/mul7dnn/logits/kernel/part_0/Initializer/random_uniform/min*
T0*+
_class!
loc:@dnn/logits/kernel/part_0*
_output_shapes

:

?
dnn/logits/kernel/part_0/AssignAssigndnn/logits/kernel/part_03dnn/logits/kernel/part_0/Initializer/random_uniform*
_output_shapes

:
*
use_locking(*
T0*+
_class!
loc:@dnn/logits/kernel/part_0*
validate_shape(
?
$dnn/dnn/hiddenlayer_2/activation/tagConst*
_output_shapes
: *1
value(B& B dnn/dnn/hiddenlayer_2/activation*
dtype0
?
2dnn/dnn/hiddenlayer_2/fraction_of_zero_values/tagsConst*>
value5B3 B-dnn/dnn/hiddenlayer_2/fraction_of_zero_values*
dtype0*
_output_shapes
: 
j
dnn/zero_fraction_2/ConstConst*
valueB"       *
dtype0*
_output_shapes
:
]
dnn/zero_fraction_2/zeroConst*
valueB
 *    *
dtype0*
_output_shapes
: 
?
dnn/hiddenlayer_2/bias/part_0
VariableV2*0
_class&
$"loc:@dnn/hiddenlayer_2/bias/part_0*
	container *
shape:
*
dtype0*
_output_shapes
:
*
shared_name 
?
save_1/Assign_4Assigndnn/hiddenlayer_2/bias/part_0save_1/RestoreV2:4*0
_class&
$"loc:@dnn/hiddenlayer_2/bias/part_0*
validate_shape(*
_output_shapes
:
*
use_locking(*
T0
?
save/Assign_4Assigndnn/hiddenlayer_2/bias/part_0save/RestoreV2:4*
use_locking(*
T0*0
_class&
$"loc:@dnn/hiddenlayer_2/bias/part_0*
validate_shape(*
_output_shapes
:

?
"dnn/hiddenlayer_2/bias/part_0/readIdentitydnn/hiddenlayer_2/bias/part_0*
_output_shapes
:
*
T0*0
_class&
$"loc:@dnn/hiddenlayer_2/bias/part_0
k
dnn/hiddenlayer_2/biasIdentity"dnn/hiddenlayer_2/bias/part_0/read*
T0*
_output_shapes
:

?
/dnn/hiddenlayer_2/bias/part_0/Initializer/zerosConst*0
_class&
$"loc:@dnn/hiddenlayer_2/bias/part_0*
valueB
*    *
dtype0*
_output_shapes
:

?
$dnn/hiddenlayer_2/bias/part_0/AssignAssigndnn/hiddenlayer_2/bias/part_0/dnn/hiddenlayer_2/bias/part_0/Initializer/zeros*
_output_shapes
:
*
use_locking(*
T0*0
_class&
$"loc:@dnn/hiddenlayer_2/bias/part_0*
validate_shape(
?
dnn/hiddenlayer_2/kernel/part_0
VariableV2*
shape
:
*
dtype0*
_output_shapes

:
*
shared_name *2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
	container 
?
save_1/Assign_5Assigndnn/hiddenlayer_2/kernel/part_0save_1/RestoreV2:5*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
validate_shape(*
_output_shapes

:
*
use_locking(*
T0
?
save/Assign_5Assigndnn/hiddenlayer_2/kernel/part_0save/RestoreV2:5*
_output_shapes

:
*
use_locking(*
T0*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
validate_shape(
?
$dnn/hiddenlayer_2/kernel/part_0/readIdentitydnn/hiddenlayer_2/kernel/part_0*
_output_shapes

:
*
T0*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0
s
dnn/hiddenlayer_2/kernelIdentity$dnn/hiddenlayer_2/kernel/part_0/read*
_output_shapes

:
*
T0
?
>dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/maxConst*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
valueB
 *.??>*
dtype0*
_output_shapes
: 
?
>dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/minConst*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
valueB
 *.???*
dtype0*
_output_shapes
: 
?
>dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/subSub>dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/max>dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/min*
T0*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
_output_shapes
: 
?
@dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/shapeConst*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
valueB"   
   *
dtype0*
_output_shapes
:
?
Hdnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/RandomUniformRandomUniform@dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/shape*

seed *
T0*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
seed2 *
dtype0*
_output_shapes

:

?
>dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/mulMulHdnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/RandomUniform>dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/sub*
T0*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
_output_shapes

:

?
:dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniformAdd>dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/mul>dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform/min*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
_output_shapes

:
*
T0
?
&dnn/hiddenlayer_2/kernel/part_0/AssignAssigndnn/hiddenlayer_2/kernel/part_0:dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform*
T0*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
validate_shape(*
_output_shapes

:
*
use_locking(
?
$dnn/dnn/hiddenlayer_1/activation/tagConst*1
value(B& B dnn/dnn/hiddenlayer_1/activation*
dtype0*
_output_shapes
: 
?
2dnn/dnn/hiddenlayer_1/fraction_of_zero_values/tagsConst*>
value5B3 B-dnn/dnn/hiddenlayer_1/fraction_of_zero_values*
dtype0*
_output_shapes
: 
j
dnn/zero_fraction_1/ConstConst*
valueB"       *
dtype0*
_output_shapes
:
]
dnn/zero_fraction_1/zeroConst*
valueB
 *    *
dtype0*
_output_shapes
: 
?
dnn/hiddenlayer_1/bias/part_0
VariableV2*
shape:*
dtype0*
_output_shapes
:*
shared_name *0
_class&
$"loc:@dnn/hiddenlayer_1/bias/part_0*
	container 
?
save_1/Assign_2Assigndnn/hiddenlayer_1/bias/part_0save_1/RestoreV2:2*
_output_shapes
:*
use_locking(*
T0*0
_class&
$"loc:@dnn/hiddenlayer_1/bias/part_0*
validate_shape(
?
save/Assign_2Assigndnn/hiddenlayer_1/bias/part_0save/RestoreV2:2*
use_locking(*
T0*0
_class&
$"loc:@dnn/hiddenlayer_1/bias/part_0*
validate_shape(*
_output_shapes
:
?
"dnn/hiddenlayer_1/bias/part_0/readIdentitydnn/hiddenlayer_1/bias/part_0*
T0*0
_class&
$"loc:@dnn/hiddenlayer_1/bias/part_0*
_output_shapes
:
k
dnn/hiddenlayer_1/biasIdentity"dnn/hiddenlayer_1/bias/part_0/read*
T0*
_output_shapes
:
?
/dnn/hiddenlayer_1/bias/part_0/Initializer/zerosConst*0
_class&
$"loc:@dnn/hiddenlayer_1/bias/part_0*
valueB*    *
dtype0*
_output_shapes
:
?
$dnn/hiddenlayer_1/bias/part_0/AssignAssigndnn/hiddenlayer_1/bias/part_0/dnn/hiddenlayer_1/bias/part_0/Initializer/zeros*
T0*0
_class&
$"loc:@dnn/hiddenlayer_1/bias/part_0*
validate_shape(*
_output_shapes
:*
use_locking(
?
dnn/hiddenlayer_1/kernel/part_0
VariableV2*
	container *
shape
:
*
dtype0*
_output_shapes

:
*
shared_name *2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0
?
save_1/Assign_3Assigndnn/hiddenlayer_1/kernel/part_0save_1/RestoreV2:3*2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0*
validate_shape(*
_output_shapes

:
*
use_locking(*
T0
?
save/Assign_3Assigndnn/hiddenlayer_1/kernel/part_0save/RestoreV2:3*2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0*
validate_shape(*
_output_shapes

:
*
use_locking(*
T0
?
$dnn/hiddenlayer_1/kernel/part_0/readIdentitydnn/hiddenlayer_1/kernel/part_0*
_output_shapes

:
*
T0*2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0
s
dnn/hiddenlayer_1/kernelIdentity$dnn/hiddenlayer_1/kernel/part_0/read*
T0*
_output_shapes

:

?
>dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/maxConst*
dtype0*
_output_shapes
: *2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0*
valueB
 *.??>
?
>dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/minConst*
_output_shapes
: *2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0*
valueB
 *.???*
dtype0
?
>dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/subSub>dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/max>dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/min*
T0*2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0*
_output_shapes
: 
?
@dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/shapeConst*2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0*
valueB"
      *
dtype0*
_output_shapes
:
?
Hdnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/RandomUniformRandomUniform@dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/shape*
dtype0*
_output_shapes

:
*

seed *
T0*2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0*
seed2 
?
>dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/mulMulHdnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/RandomUniform>dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/sub*
T0*2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0*
_output_shapes

:

?
:dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniformAdd>dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/mul>dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform/min*
_output_shapes

:
*
T0*2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0
?
&dnn/hiddenlayer_1/kernel/part_0/AssignAssigndnn/hiddenlayer_1/kernel/part_0:dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform*
use_locking(*
T0*2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0*
validate_shape(*
_output_shapes

:

?
$dnn/dnn/hiddenlayer_0/activation/tagConst*1
value(B& B dnn/dnn/hiddenlayer_0/activation*
dtype0*
_output_shapes
: 
?
2dnn/dnn/hiddenlayer_0/fraction_of_zero_values/tagsConst*>
value5B3 B-dnn/dnn/hiddenlayer_0/fraction_of_zero_values*
dtype0*
_output_shapes
: 
h
dnn/zero_fraction/ConstConst*
valueB"       *
dtype0*
_output_shapes
:
[
dnn/zero_fraction/zeroConst*
_output_shapes
: *
valueB
 *    *
dtype0
?
dnn/hiddenlayer_0/bias/part_0
VariableV2*
dtype0*
_output_shapes
:
*
shared_name *0
_class&
$"loc:@dnn/hiddenlayer_0/bias/part_0*
	container *
shape:

?
save_1/AssignAssigndnn/hiddenlayer_0/bias/part_0save_1/RestoreV2*
T0*0
_class&
$"loc:@dnn/hiddenlayer_0/bias/part_0*
validate_shape(*
_output_shapes
:
*
use_locking(
?
save/AssignAssigndnn/hiddenlayer_0/bias/part_0save/RestoreV2*
use_locking(*
T0*0
_class&
$"loc:@dnn/hiddenlayer_0/bias/part_0*
validate_shape(*
_output_shapes
:

?
"dnn/hiddenlayer_0/bias/part_0/readIdentitydnn/hiddenlayer_0/bias/part_0*
_output_shapes
:
*
T0*0
_class&
$"loc:@dnn/hiddenlayer_0/bias/part_0
k
dnn/hiddenlayer_0/biasIdentity"dnn/hiddenlayer_0/bias/part_0/read*
T0*
_output_shapes
:

?
/dnn/hiddenlayer_0/bias/part_0/Initializer/zerosConst*0
_class&
$"loc:@dnn/hiddenlayer_0/bias/part_0*
valueB
*    *
dtype0*
_output_shapes
:

?
$dnn/hiddenlayer_0/bias/part_0/AssignAssigndnn/hiddenlayer_0/bias/part_0/dnn/hiddenlayer_0/bias/part_0/Initializer/zeros*
T0*0
_class&
$"loc:@dnn/hiddenlayer_0/bias/part_0*
validate_shape(*
_output_shapes
:
*
use_locking(
?
dnn/hiddenlayer_0/kernel/part_0
VariableV2*
dtype0*
_output_shapes

:
*
shared_name *2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
	container *
shape
:

?
save_1/Assign_1Assigndnn/hiddenlayer_0/kernel/part_0save_1/RestoreV2:1*
use_locking(*
T0*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
validate_shape(*
_output_shapes

:

?
save/Assign_1Assigndnn/hiddenlayer_0/kernel/part_0save/RestoreV2:1*
T0*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
validate_shape(*
_output_shapes

:
*
use_locking(
?
$dnn/hiddenlayer_0/kernel/part_0/readIdentitydnn/hiddenlayer_0/kernel/part_0*
T0*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
_output_shapes

:

s
dnn/hiddenlayer_0/kernelIdentity$dnn/hiddenlayer_0/kernel/part_0/read*
_output_shapes

:
*
T0
?
>dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/maxConst*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
valueB
 *b?'?*
dtype0*
_output_shapes
: 
?
>dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/minConst*
_output_shapes
: *2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
valueB
 *b?'?*
dtype0
?
>dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/subSub>dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/max>dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/min*
T0*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
_output_shapes
: 
?
@dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/shapeConst*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
valueB"   
   *
dtype0*
_output_shapes
:
?
Hdnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/RandomUniformRandomUniform@dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/shape*
T0*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
seed2 *
dtype0*
_output_shapes

:
*

seed 
?
>dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/mulMulHdnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/RandomUniform>dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/sub*
_output_shapes

:
*
T0*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0
?
:dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniformAdd>dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/mul>dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform/min*
T0*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
_output_shapes

:

?
&dnn/hiddenlayer_0/kernel/part_0/AssignAssigndnn/hiddenlayer_0/kernel/part_0:dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
validate_shape(*
_output_shapes

:
*
use_locking(*
T0
x
6dnn/input_from_feature_columns/input_layer/concat/axisConst*
value	B :*
dtype0*
_output_shapes
: 
?
Ednn/input_from_feature_columns/input_layer/SepalWidth/Reshape/shape/1Const*
value	B :*
dtype0*
_output_shapes
: 
?
Kdnn/input_from_feature_columns/input_layer/SepalWidth/strided_slice/stack_2Const*
dtype0*
_output_shapes
:*
valueB:
?
Kdnn/input_from_feature_columns/input_layer/SepalWidth/strided_slice/stack_1Const*
valueB:*
dtype0*
_output_shapes
:
?
Idnn/input_from_feature_columns/input_layer/SepalWidth/strided_slice/stackConst*
valueB: *
dtype0*
_output_shapes
:
?
Fdnn/input_from_feature_columns/input_layer/SepalLength/Reshape/shape/1Const*
value	B :*
dtype0*
_output_shapes
: 
?
Ldnn/input_from_feature_columns/input_layer/SepalLength/strided_slice/stack_2Const*
valueB:*
dtype0*
_output_shapes
:
?
Ldnn/input_from_feature_columns/input_layer/SepalLength/strided_slice/stack_1Const*
valueB:*
dtype0*
_output_shapes
:
?
Jdnn/input_from_feature_columns/input_layer/SepalLength/strided_slice/stackConst*
valueB: *
dtype0*
_output_shapes
:
?
Ednn/input_from_feature_columns/input_layer/PetalWidth/Reshape/shape/1Const*
_output_shapes
: *
value	B :*
dtype0
?
Kdnn/input_from_feature_columns/input_layer/PetalWidth/strided_slice/stack_2Const*
_output_shapes
:*
valueB:*
dtype0
?
Kdnn/input_from_feature_columns/input_layer/PetalWidth/strided_slice/stack_1Const*
valueB:*
dtype0*
_output_shapes
:
?
Idnn/input_from_feature_columns/input_layer/PetalWidth/strided_slice/stackConst*
valueB: *
dtype0*
_output_shapes
:
?
Fdnn/input_from_feature_columns/input_layer/PetalLength/Reshape/shape/1Const*
_output_shapes
: *
value	B :*
dtype0
?
Ldnn/input_from_feature_columns/input_layer/PetalLength/strided_slice/stack_2Const*
_output_shapes
:*
valueB:*
dtype0
?
Ldnn/input_from_feature_columns/input_layer/PetalLength/strided_slice/stack_1Const*
valueB:*
dtype0*
_output_shapes
:
?
Jdnn/input_from_feature_columns/input_layer/PetalLength/strided_slice/stackConst*
_output_shapes
:*
valueB: *
dtype0
m

SepalWidthPlaceholder*
shape:?????????*
dtype0*'
_output_shapes
:?????????
?
;dnn/input_from_feature_columns/input_layer/SepalWidth/ShapeShape
SepalWidth*
T0*
out_type0*
_output_shapes
:
?
Cdnn/input_from_feature_columns/input_layer/SepalWidth/strided_sliceStridedSlice;dnn/input_from_feature_columns/input_layer/SepalWidth/ShapeIdnn/input_from_feature_columns/input_layer/SepalWidth/strided_slice/stackKdnn/input_from_feature_columns/input_layer/SepalWidth/strided_slice/stack_1Kdnn/input_from_feature_columns/input_layer/SepalWidth/strided_slice/stack_2*
_output_shapes
: *
T0*
Index0*
shrink_axis_mask*

begin_mask *
ellipsis_mask *
new_axis_mask *
end_mask 
?
Cdnn/input_from_feature_columns/input_layer/SepalWidth/Reshape/shapePackCdnn/input_from_feature_columns/input_layer/SepalWidth/strided_sliceEdnn/input_from_feature_columns/input_layer/SepalWidth/Reshape/shape/1*
T0*

axis *
N*
_output_shapes
:
?
=dnn/input_from_feature_columns/input_layer/SepalWidth/ReshapeReshape
SepalWidthCdnn/input_from_feature_columns/input_layer/SepalWidth/Reshape/shape*
T0*
Tshape0*'
_output_shapes
:?????????
n
SepalLengthPlaceholder*'
_output_shapes
:?????????*
shape:?????????*
dtype0
?
<dnn/input_from_feature_columns/input_layer/SepalLength/ShapeShapeSepalLength*
T0*
out_type0*
_output_shapes
:
?
Ddnn/input_from_feature_columns/input_layer/SepalLength/strided_sliceStridedSlice<dnn/input_from_feature_columns/input_layer/SepalLength/ShapeJdnn/input_from_feature_columns/input_layer/SepalLength/strided_slice/stackLdnn/input_from_feature_columns/input_layer/SepalLength/strided_slice/stack_1Ldnn/input_from_feature_columns/input_layer/SepalLength/strided_slice/stack_2*
Index0*
T0*
shrink_axis_mask*
ellipsis_mask *

begin_mask *
new_axis_mask *
end_mask *
_output_shapes
: 
?
Ddnn/input_from_feature_columns/input_layer/SepalLength/Reshape/shapePackDdnn/input_from_feature_columns/input_layer/SepalLength/strided_sliceFdnn/input_from_feature_columns/input_layer/SepalLength/Reshape/shape/1*
_output_shapes
:*
T0*

axis *
N
?
>dnn/input_from_feature_columns/input_layer/SepalLength/ReshapeReshapeSepalLengthDdnn/input_from_feature_columns/input_layer/SepalLength/Reshape/shape*
T0*
Tshape0*'
_output_shapes
:?????????
m

PetalWidthPlaceholder*
dtype0*'
_output_shapes
:?????????*
shape:?????????
?
;dnn/input_from_feature_columns/input_layer/PetalWidth/ShapeShape
PetalWidth*
_output_shapes
:*
T0*
out_type0
?
Cdnn/input_from_feature_columns/input_layer/PetalWidth/strided_sliceStridedSlice;dnn/input_from_feature_columns/input_layer/PetalWidth/ShapeIdnn/input_from_feature_columns/input_layer/PetalWidth/strided_slice/stackKdnn/input_from_feature_columns/input_layer/PetalWidth/strided_slice/stack_1Kdnn/input_from_feature_columns/input_layer/PetalWidth/strided_slice/stack_2*
shrink_axis_mask*
ellipsis_mask *

begin_mask *
new_axis_mask *
end_mask *
_output_shapes
: *
T0*
Index0
?
Cdnn/input_from_feature_columns/input_layer/PetalWidth/Reshape/shapePackCdnn/input_from_feature_columns/input_layer/PetalWidth/strided_sliceEdnn/input_from_feature_columns/input_layer/PetalWidth/Reshape/shape/1*
T0*

axis *
N*
_output_shapes
:
?
=dnn/input_from_feature_columns/input_layer/PetalWidth/ReshapeReshape
PetalWidthCdnn/input_from_feature_columns/input_layer/PetalWidth/Reshape/shape*'
_output_shapes
:?????????*
T0*
Tshape0
n
PetalLengthPlaceholder*
dtype0*'
_output_shapes
:?????????*
shape:?????????
?
<dnn/input_from_feature_columns/input_layer/PetalLength/ShapeShapePetalLength*
T0*
out_type0*
_output_shapes
:
?
Ddnn/input_from_feature_columns/input_layer/PetalLength/strided_sliceStridedSlice<dnn/input_from_feature_columns/input_layer/PetalLength/ShapeJdnn/input_from_feature_columns/input_layer/PetalLength/strided_slice/stackLdnn/input_from_feature_columns/input_layer/PetalLength/strided_slice/stack_1Ldnn/input_from_feature_columns/input_layer/PetalLength/strided_slice/stack_2*
shrink_axis_mask*
ellipsis_mask *

begin_mask *
new_axis_mask *
end_mask *
_output_shapes
: *
Index0*
T0
?
Ddnn/input_from_feature_columns/input_layer/PetalLength/Reshape/shapePackDdnn/input_from_feature_columns/input_layer/PetalLength/strided_sliceFdnn/input_from_feature_columns/input_layer/PetalLength/Reshape/shape/1*

axis *
N*
_output_shapes
:*
T0
?
>dnn/input_from_feature_columns/input_layer/PetalLength/ReshapeReshapePetalLengthDdnn/input_from_feature_columns/input_layer/PetalLength/Reshape/shape*
T0*
Tshape0*'
_output_shapes
:?????????
?
1dnn/input_from_feature_columns/input_layer/concatConcatV2>dnn/input_from_feature_columns/input_layer/PetalLength/Reshape=dnn/input_from_feature_columns/input_layer/PetalWidth/Reshape>dnn/input_from_feature_columns/input_layer/SepalLength/Reshape=dnn/input_from_feature_columns/input_layer/SepalWidth/Reshape6dnn/input_from_feature_columns/input_layer/concat/axis*
T0*
N*'
_output_shapes
:?????????*

Tidx0
?
dnn/hiddenlayer_0/MatMulMatMul1dnn/input_from_feature_columns/input_layer/concatdnn/hiddenlayer_0/kernel*'
_output_shapes
:?????????
*
transpose_a( *
transpose_b( *
T0
?
dnn/hiddenlayer_0/BiasAddBiasAdddnn/hiddenlayer_0/MatMuldnn/hiddenlayer_0/bias*
T0*
data_formatNHWC*'
_output_shapes
:?????????

k
dnn/hiddenlayer_0/ReluReludnn/hiddenlayer_0/BiasAdd*'
_output_shapes
:?????????
*
T0
?
dnn/hiddenlayer_1/MatMulMatMuldnn/hiddenlayer_0/Reludnn/hiddenlayer_1/kernel*'
_output_shapes
:?????????*
transpose_a( *
transpose_b( *
T0
?
dnn/hiddenlayer_1/BiasAddBiasAdddnn/hiddenlayer_1/MatMuldnn/hiddenlayer_1/bias*
T0*
data_formatNHWC*'
_output_shapes
:?????????
k
dnn/hiddenlayer_1/ReluReludnn/hiddenlayer_1/BiasAdd*
T0*'
_output_shapes
:?????????
?
dnn/hiddenlayer_2/MatMulMatMuldnn/hiddenlayer_1/Reludnn/hiddenlayer_2/kernel*'
_output_shapes
:?????????
*
transpose_a( *
transpose_b( *
T0
?
dnn/hiddenlayer_2/BiasAddBiasAdddnn/hiddenlayer_2/MatMuldnn/hiddenlayer_2/bias*
T0*
data_formatNHWC*'
_output_shapes
:?????????

k
dnn/hiddenlayer_2/ReluReludnn/hiddenlayer_2/BiasAdd*'
_output_shapes
:?????????
*
T0
?
dnn/logits/MatMulMatMuldnn/hiddenlayer_2/Reludnn/logits/kernel*
T0*'
_output_shapes
:?????????*
transpose_a( *
transpose_b( 
?
dnn/logits/BiasAddBiasAdddnn/logits/MatMuldnn/logits/bias*
T0*
data_formatNHWC*'
_output_shapes
:?????????
s
"dnn/head/predictions/probabilitiesSoftmaxdnn/logits/BiasAdd*
T0*'
_output_shapes
:?????????
p
dnn/head/ShapeShape"dnn/head/predictions/probabilities*
out_type0*
_output_shapes
:*
T0
?
dnn/head/strided_sliceStridedSlicednn/head/Shapednn/head/strided_slice/stackdnn/head/strided_slice/stack_1dnn/head/strided_slice/stack_2*
new_axis_mask *
end_mask *
_output_shapes
: *
Index0*
T0*
shrink_axis_mask*

begin_mask *
ellipsis_mask 
?
dnn/head/Tile/multiplesPackdnn/head/strided_slicednn/head/Tile/multiples/1*
T0*

axis *
N*
_output_shapes
:
?
dnn/head/TileTilednn/head/ExpandDimsdnn/head/Tile/multiples*'
_output_shapes
:?????????*

Tmultiples0*
T0
?
dnn/head/predictions/class_idsArgMaxdnn/logits/BiasAdd(dnn/head/predictions/class_ids/dimension*

Tidx0*
T0*
output_type0	*#
_output_shapes
:?????????
?
dnn/head/predictions/ExpandDims
ExpandDimsdnn/head/predictions/class_ids#dnn/head/predictions/ExpandDims/dim*'
_output_shapes
:?????????*

Tdim0*
T0	
?
 dnn/head/predictions/str_classesAsStringdnn/head/predictions/ExpandDims*'
_output_shapes
:?????????*
	precision?????????*
shortest( *
T0	*

fill *

scientific( *
width?????????
g
dnn/head/logits/ShapeShapednn/logits/BiasAdd*
T0*
out_type0*
_output_shapes
:
?
dnn/dnn/logits/activationHistogramSummarydnn/dnn/logits/activation/tagdnn/logits/BiasAdd*
_output_shapes
: *
T0
?
dnn/zero_fraction_3/EqualEqualdnn/logits/BiasAdddnn/zero_fraction_3/zero*
T0*'
_output_shapes
:?????????
|
dnn/zero_fraction_3/CastCastdnn/zero_fraction_3/Equal*'
_output_shapes
:?????????*

DstT0*

SrcT0

?
dnn/zero_fraction_3/MeanMeandnn/zero_fraction_3/Castdnn/zero_fraction_3/Const*
_output_shapes
: *
	keep_dims( *

Tidx0*
T0
?
&dnn/dnn/logits/fraction_of_zero_valuesScalarSummary+dnn/dnn/logits/fraction_of_zero_values/tagsdnn/zero_fraction_3/Mean*
T0*
_output_shapes
: 
?
 dnn/dnn/hiddenlayer_2/activationHistogramSummary$dnn/dnn/hiddenlayer_2/activation/tagdnn/hiddenlayer_2/Relu*
T0*
_output_shapes
: 
?
dnn/zero_fraction_2/EqualEqualdnn/hiddenlayer_2/Reludnn/zero_fraction_2/zero*'
_output_shapes
:?????????
*
T0
|
dnn/zero_fraction_2/CastCastdnn/zero_fraction_2/Equal*'
_output_shapes
:?????????
*

DstT0*

SrcT0

?
dnn/zero_fraction_2/MeanMeandnn/zero_fraction_2/Castdnn/zero_fraction_2/Const*
_output_shapes
: *
	keep_dims( *

Tidx0*
T0
?
-dnn/dnn/hiddenlayer_2/fraction_of_zero_valuesScalarSummary2dnn/dnn/hiddenlayer_2/fraction_of_zero_values/tagsdnn/zero_fraction_2/Mean*
T0*
_output_shapes
: 
?
 dnn/dnn/hiddenlayer_1/activationHistogramSummary$dnn/dnn/hiddenlayer_1/activation/tagdnn/hiddenlayer_1/Relu*
_output_shapes
: *
T0
?
dnn/zero_fraction_1/EqualEqualdnn/hiddenlayer_1/Reludnn/zero_fraction_1/zero*
T0*'
_output_shapes
:?????????
|
dnn/zero_fraction_1/CastCastdnn/zero_fraction_1/Equal*

SrcT0
*'
_output_shapes
:?????????*

DstT0
?
dnn/zero_fraction_1/MeanMeandnn/zero_fraction_1/Castdnn/zero_fraction_1/Const*
_output_shapes
: *
	keep_dims( *

Tidx0*
T0
?
-dnn/dnn/hiddenlayer_1/fraction_of_zero_valuesScalarSummary2dnn/dnn/hiddenlayer_1/fraction_of_zero_values/tagsdnn/zero_fraction_1/Mean*
_output_shapes
: *
T0
?
 dnn/dnn/hiddenlayer_0/activationHistogramSummary$dnn/dnn/hiddenlayer_0/activation/tagdnn/hiddenlayer_0/Relu*
T0*
_output_shapes
: 
?
dnn/zero_fraction/EqualEqualdnn/hiddenlayer_0/Reludnn/zero_fraction/zero*
T0*'
_output_shapes
:?????????

x
dnn/zero_fraction/CastCastdnn/zero_fraction/Equal*

SrcT0
*'
_output_shapes
:?????????
*

DstT0
?
dnn/zero_fraction/MeanMeandnn/zero_fraction/Castdnn/zero_fraction/Const*
	keep_dims( *

Tidx0*
T0*
_output_shapes
: 
?
-dnn/dnn/hiddenlayer_0/fraction_of_zero_valuesScalarSummary2dnn/dnn/hiddenlayer_0/fraction_of_zero_values/tagsdnn/zero_fraction/Mean*
T0*
_output_shapes
: 
?
global_step
VariableV2*
dtype0	*
_output_shapes
: *
shared_name *
_class
loc:@global_step*
	container *
shape: 
?
save_1/Assign_8Assignglobal_stepsave_1/RestoreV2:8*
T0	*
_class
loc:@global_step*
validate_shape(*
_output_shapes
: *
use_locking(
?
save_1/restore_shardNoOp^save_1/Assign^save_1/Assign_1^save_1/Assign_2^save_1/Assign_3^save_1/Assign_4^save_1/Assign_5^save_1/Assign_6^save_1/Assign_7^save_1/Assign_8
1
save_1/restore_allNoOp^save_1/restore_shard
?
save_1/SaveV2SaveV2save_1/ShardedFilenamesave_1/SaveV2/tensor_namessave_1/SaveV2/shape_and_slices"dnn/hiddenlayer_0/bias/part_0/read$dnn/hiddenlayer_0/kernel/part_0/read"dnn/hiddenlayer_1/bias/part_0/read$dnn/hiddenlayer_1/kernel/part_0/read"dnn/hiddenlayer_2/bias/part_0/read$dnn/hiddenlayer_2/kernel/part_0/readdnn/logits/bias/part_0/readdnn/logits/kernel/part_0/readglobal_step"/device:CPU:0*
dtypes
2		
?
save_1/control_dependencyIdentitysave_1/ShardedFilename^save_1/SaveV2"/device:CPU:0*)
_class
loc:@save_1/ShardedFilename*
_output_shapes
: *
T0
?
-save_1/MergeV2Checkpoints/checkpoint_prefixesPacksave_1/ShardedFilename^save_1/control_dependency"/device:CPU:0*
_output_shapes
:*
T0*

axis *
N
?
save_1/MergeV2CheckpointsMergeV2Checkpoints-save_1/MergeV2Checkpoints/checkpoint_prefixessave_1/Const"/device:CPU:0*
delete_old_dirs(
?
save_1/IdentityIdentitysave_1/Const^save_1/MergeV2Checkpoints^save_1/control_dependency"/device:CPU:0*
_output_shapes
: *
T0
?
save/Assign_8Assignglobal_stepsave/RestoreV2:8*
use_locking(*
T0	*
_class
loc:@global_step*
validate_shape(*
_output_shapes
: 
?
save/restore_shardNoOp^save/Assign^save/Assign_1^save/Assign_2^save/Assign_3^save/Assign_4^save/Assign_5^save/Assign_6^save/Assign_7^save/Assign_8
-
save/restore_allNoOp^save/restore_shard
?
save/SaveV2SaveV2save/ShardedFilenamesave/SaveV2/tensor_namessave/SaveV2/shape_and_slices"dnn/hiddenlayer_0/bias/part_0/read$dnn/hiddenlayer_0/kernel/part_0/read"dnn/hiddenlayer_1/bias/part_0/read$dnn/hiddenlayer_1/kernel/part_0/read"dnn/hiddenlayer_2/bias/part_0/read$dnn/hiddenlayer_2/kernel/part_0/readdnn/logits/bias/part_0/readdnn/logits/kernel/part_0/readglobal_step"/device:CPU:0*
dtypes
2		
?
save/control_dependencyIdentitysave/ShardedFilename^save/SaveV2"/device:CPU:0*
T0*'
_class
loc:@save/ShardedFilename*
_output_shapes
: 
?
+save/MergeV2Checkpoints/checkpoint_prefixesPacksave/ShardedFilename^save/control_dependency"/device:CPU:0*
T0*

axis *
N*
_output_shapes
:
?
save/MergeV2CheckpointsMergeV2Checkpoints+save/MergeV2Checkpoints/checkpoint_prefixes
save/Const"/device:CPU:0*
delete_old_dirs(
?
save/IdentityIdentity
save/Const^save/MergeV2Checkpoints^save/control_dependency"/device:CPU:0*
T0*
_output_shapes
: 
j
global_step/readIdentityglobal_step*
T0	*
_class
loc:@global_step*
_output_shapes
: 

global_step/Initializer/zerosConst*
_class
loc:@global_step*
value	B	 R *
dtype0	*
_output_shapes
: 
?
global_step/AssignAssignglobal_stepglobal_step/Initializer/zeros*
use_locking(*
T0	*
_class
loc:@global_step*
validate_shape(*
_output_shapes
: 
R
save/Const_1Const*
valueB Bmodel*
dtype0*
_output_shapes
: 
?
save/StringJoin_1/inputs_1Const*<
value3B1 B+_temp_2ccdbb9d257f4ac3b24426fe398faaf1/part*
dtype0*
_output_shapes
: 
{
save/StringJoin_1
StringJoinsave/Const_1save/StringJoin_1/inputs_1*
	separator *
N*
_output_shapes
: 
S
save/num_shards_1Const*
value	B :*
dtype0*
_output_shapes
: 
m
save/ShardedFilename_1/shardConst"/device:CPU:0*
value	B : *
dtype0*
_output_shapes
: 
?
save/ShardedFilename_1ShardedFilenamesave/StringJoin_1save/ShardedFilename_1/shardsave/num_shards_1"/device:CPU:0*
_output_shapes
: 
?
save/SaveV2_1/tensor_namesConst"/device:CPU:0*
dtype0*
_output_shapes
:	*?
value?B?	Bdnn/hiddenlayer_0/biasBdnn/hiddenlayer_0/kernelBdnn/hiddenlayer_1/biasBdnn/hiddenlayer_1/kernelBdnn/hiddenlayer_2/biasBdnn/hiddenlayer_2/kernelBdnn/logits/biasBdnn/logits/kernelBglobal_step
?
save/SaveV2_1/shape_and_slicesConst"/device:CPU:0*w
valuenBl	B10 0,10B4 10 0,4:0,10B20 0,20B10 20 0,10:0,20B10 0,10B20 10 0,20:0,10B3 0,3B10 3 0,10:0,3B *
dtype0*
_output_shapes
:	
?
save/SaveV2_1SaveV2save/ShardedFilename_1save/SaveV2_1/tensor_namessave/SaveV2_1/shape_and_slices"dnn/hiddenlayer_0/bias/part_0/read$dnn/hiddenlayer_0/kernel/part_0/read"dnn/hiddenlayer_1/bias/part_0/read$dnn/hiddenlayer_1/kernel/part_0/read"dnn/hiddenlayer_2/bias/part_0/read$dnn/hiddenlayer_2/kernel/part_0/readdnn/logits/bias/part_0/readdnn/logits/kernel/part_0/readglobal_step"/device:CPU:0*
dtypes
2		
?
save/control_dependency_1Identitysave/ShardedFilename_1^save/SaveV2_1"/device:CPU:0*)
_class
loc:@save/ShardedFilename_1*
_output_shapes
: *
T0
?
-save/MergeV2Checkpoints_1/checkpoint_prefixesPacksave/ShardedFilename_1^save/control_dependency_1"/device:CPU:0*
_output_shapes
:*
T0*

axis *
N
?
save/MergeV2Checkpoints_1MergeV2Checkpoints-save/MergeV2Checkpoints_1/checkpoint_prefixessave/Const_1"/device:CPU:0*
delete_old_dirs(
?
save/Identity_1Identitysave/Const_1^save/MergeV2Checkpoints_1^save/control_dependency_1"/device:CPU:0*
T0*
_output_shapes
: 
?
save/RestoreV2_1/tensor_namesConst"/device:CPU:0*?
value?B?	Bdnn/hiddenlayer_0/biasBdnn/hiddenlayer_0/kernelBdnn/hiddenlayer_1/biasBdnn/hiddenlayer_1/kernelBdnn/hiddenlayer_2/biasBdnn/hiddenlayer_2/kernelBdnn/logits/biasBdnn/logits/kernelBglobal_step*
dtype0*
_output_shapes
:	
?
!save/RestoreV2_1/shape_and_slicesConst"/device:CPU:0*w
valuenBl	B10 0,10B4 10 0,4:0,10B20 0,20B10 20 0,10:0,20B10 0,10B20 10 0,20:0,10B3 0,3B10 3 0,10:0,3B *
dtype0*
_output_shapes
:	
?
save/RestoreV2_1	RestoreV2save/Const_1save/RestoreV2_1/tensor_names!save/RestoreV2_1/shape_and_slices"/device:CPU:0*X
_output_shapesF
D:
:
::
:
:
::
:*
dtypes
2		
?
save/Assign_9Assigndnn/hiddenlayer_0/bias/part_0save/RestoreV2_1*
_output_shapes
:
*
use_locking(*
T0*0
_class&
$"loc:@dnn/hiddenlayer_0/bias/part_0*
validate_shape(
?
save/Assign_10Assigndnn/hiddenlayer_0/kernel/part_0save/RestoreV2_1:1*
use_locking(*
T0*2
_class(
&$loc:@dnn/hiddenlayer_0/kernel/part_0*
validate_shape(*
_output_shapes

:

?
save/Assign_11Assigndnn/hiddenlayer_1/bias/part_0save/RestoreV2_1:2*
use_locking(*
T0*0
_class&
$"loc:@dnn/hiddenlayer_1/bias/part_0*
validate_shape(*
_output_shapes
:
?
save/Assign_12Assigndnn/hiddenlayer_1/kernel/part_0save/RestoreV2_1:3*
use_locking(*
T0*2
_class(
&$loc:@dnn/hiddenlayer_1/kernel/part_0*
validate_shape(*
_output_shapes

:

?
save/Assign_13Assigndnn/hiddenlayer_2/bias/part_0save/RestoreV2_1:4*
use_locking(*
T0*0
_class&
$"loc:@dnn/hiddenlayer_2/bias/part_0*
validate_shape(*
_output_shapes
:

?
save/Assign_14Assigndnn/hiddenlayer_2/kernel/part_0save/RestoreV2_1:5*2
_class(
&$loc:@dnn/hiddenlayer_2/kernel/part_0*
validate_shape(*
_output_shapes

:
*
use_locking(*
T0
?
save/Assign_15Assigndnn/logits/bias/part_0save/RestoreV2_1:6*
use_locking(*
T0*)
_class
loc:@dnn/logits/bias/part_0*
validate_shape(*
_output_shapes
:
?
save/Assign_16Assigndnn/logits/kernel/part_0save/RestoreV2_1:7*
_output_shapes

:
*
use_locking(*
T0*+
_class!
loc:@dnn/logits/kernel/part_0*
validate_shape(
?
save/Assign_17Assignglobal_stepsave/RestoreV2_1:8*
use_locking(*
T0	*
_class
loc:@global_step*
validate_shape(*
_output_shapes
: 
?
save/restore_shard_1NoOp^save/Assign_10^save/Assign_11^save/Assign_12^save/Assign_13^save/Assign_14^save/Assign_15^save/Assign_16^save/Assign_17^save/Assign_9
1
save/restore_all_1NoOp^save/restore_shard_1"B
save/Const_1:0save/Identity_1:0save/restore_all_1 (5 @F8"?
trainable_variables??
?
!dnn/hiddenlayer_0/kernel/part_0:0&dnn/hiddenlayer_0/kernel/part_0/Assign&dnn/hiddenlayer_0/kernel/part_0/read:0"&
dnn/hiddenlayer_0/kernel
  "
2<dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform:0
?
dnn/hiddenlayer_0/bias/part_0:0$dnn/hiddenlayer_0/bias/part_0/Assign$dnn/hiddenlayer_0/bias/part_0/read:0"!
dnn/hiddenlayer_0/bias
 "
21dnn/hiddenlayer_0/bias/part_0/Initializer/zeros:0
?
!dnn/hiddenlayer_1/kernel/part_0:0&dnn/hiddenlayer_1/kernel/part_0/Assign&dnn/hiddenlayer_1/kernel/part_0/read:0"&
dnn/hiddenlayer_1/kernel
  "
2<dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform:0
?
dnn/hiddenlayer_1/bias/part_0:0$dnn/hiddenlayer_1/bias/part_0/Assign$dnn/hiddenlayer_1/bias/part_0/read:0"!
dnn/hiddenlayer_1/bias "21dnn/hiddenlayer_1/bias/part_0/Initializer/zeros:0
?
!dnn/hiddenlayer_2/kernel/part_0:0&dnn/hiddenlayer_2/kernel/part_0/Assign&dnn/hiddenlayer_2/kernel/part_0/read:0"&
dnn/hiddenlayer_2/kernel
  "
2<dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform:0
?
dnn/hiddenlayer_2/bias/part_0:0$dnn/hiddenlayer_2/bias/part_0/Assign$dnn/hiddenlayer_2/bias/part_0/read:0"!
dnn/hiddenlayer_2/bias
 "
21dnn/hiddenlayer_2/bias/part_0/Initializer/zeros:0
?
dnn/logits/kernel/part_0:0dnn/logits/kernel/part_0/Assigndnn/logits/kernel/part_0/read:0"
dnn/logits/kernel
  "
25dnn/logits/kernel/part_0/Initializer/random_uniform:0
?
dnn/logits/bias/part_0:0dnn/logits/bias/part_0/Assigndnn/logits/bias/part_0/read:0"
dnn/logits/bias "2*dnn/logits/bias/part_0/Initializer/zeros:0"?
	summaries?
?
/dnn/dnn/hiddenlayer_0/fraction_of_zero_values:0
"dnn/dnn/hiddenlayer_0/activation:0
/dnn/dnn/hiddenlayer_1/fraction_of_zero_values:0
"dnn/dnn/hiddenlayer_1/activation:0
/dnn/dnn/hiddenlayer_2/fraction_of_zero_values:0
"dnn/dnn/hiddenlayer_2/activation:0
(dnn/dnn/logits/fraction_of_zero_values:0
dnn/dnn/logits/activation:0"k
global_step\Z
X
global_step:0global_step/Assignglobal_step/read:02global_step/Initializer/zeros:0"?
	variables??
X
global_step:0global_step/Assignglobal_step/read:02global_step/Initializer/zeros:0
?
!dnn/hiddenlayer_0/kernel/part_0:0&dnn/hiddenlayer_0/kernel/part_0/Assign&dnn/hiddenlayer_0/kernel/part_0/read:0"&
dnn/hiddenlayer_0/kernel
  "
2<dnn/hiddenlayer_0/kernel/part_0/Initializer/random_uniform:0
?
dnn/hiddenlayer_0/bias/part_0:0$dnn/hiddenlayer_0/bias/part_0/Assign$dnn/hiddenlayer_0/bias/part_0/read:0"!
dnn/hiddenlayer_0/bias
 "
21dnn/hiddenlayer_0/bias/part_0/Initializer/zeros:0
?
!dnn/hiddenlayer_1/kernel/part_0:0&dnn/hiddenlayer_1/kernel/part_0/Assign&dnn/hiddenlayer_1/kernel/part_0/read:0"&
dnn/hiddenlayer_1/kernel
  "
2<dnn/hiddenlayer_1/kernel/part_0/Initializer/random_uniform:0
?
dnn/hiddenlayer_1/bias/part_0:0$dnn/hiddenlayer_1/bias/part_0/Assign$dnn/hiddenlayer_1/bias/part_0/read:0"!
dnn/hiddenlayer_1/bias "21dnn/hiddenlayer_1/bias/part_0/Initializer/zeros:0
?
!dnn/hiddenlayer_2/kernel/part_0:0&dnn/hiddenlayer_2/kernel/part_0/Assign&dnn/hiddenlayer_2/kernel/part_0/read:0"&
dnn/hiddenlayer_2/kernel
  "
2<dnn/hiddenlayer_2/kernel/part_0/Initializer/random_uniform:0
?
dnn/hiddenlayer_2/bias/part_0:0$dnn/hiddenlayer_2/bias/part_0/Assign$dnn/hiddenlayer_2/bias/part_0/read:0"!
dnn/hiddenlayer_2/bias
 "
21dnn/hiddenlayer_2/bias/part_0/Initializer/zeros:0
?
dnn/logits/kernel/part_0:0dnn/logits/kernel/part_0/Assigndnn/logits/kernel/part_0/read:0"
dnn/logits/kernel
  "
25dnn/logits/kernel/part_0/Initializer/random_uniform:0
?
dnn/logits/bias/part_0:0dnn/logits/bias/part_0/Assigndnn/logits/bias/part_0/read:0"
dnn/logits/bias "2*dnn/logits/bias/part_0/Initializer/zeros:0" 
legacy_init_op


group_deps*?
serving_default?
1

PetalWidth#
PetalWidth:0?????????
1

SepalWidth#
SepalWidth:0?????????
3
SepalLength$
SepalLength:0?????????
3
PetalLength$
PetalLength:0?????????L
probabilities;
$dnn/head/predictions/probabilities:0?????????N
predicted_class_id8
!dnn/head/predictions/ExpandDims:0	?????????tensorflow/serving/predict