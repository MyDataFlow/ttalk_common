-module(ttalk_cert).
-author("David.Alpha.Fox@gmail.com").

-include_lib("public_key/include/public_key.hrl").

-export([public_key/1]).

public_key(RawX509) ->
	OTPX509 = decode_otp(RawX509),
	#'OTPCertificate'{tbsCertificate = TBS} = OTPX509,
	PublicKey = TBS#'OTPTBSCertificate'.subjectPublicKeyInfo,
	#'RSAPublicKey'{modulus = N,publicExponent = E} = PublicKey#'OTPSubjectPublicKeyInfo'.subjectPublicKey,
	[E,N].

decode_otp(Bin)->
	[{'Certificate', Data, not_encrypted}] = public_key:pem_decode(Bin),
	public_key:pkix_decode_cert(Data, otp).
