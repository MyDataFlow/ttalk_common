-module(ttalk_cert).
-author("David.Alpha.Fox@gmail.com").

-include_lib("public_key/include/public_key.hrl").

-export([public_key/1]).

public_key(RawX509) ->
	OTPX509 = decode_otp(RawX509),
	#'OTPCertificate'{tbsCertificate = TBS} = OTPX509,
    Signature = TBS#'OTPTBSCertificate'.signature,
    Algorithm = Signature#'SignatureAlgorithm'.algorithm,
    public_key(Algorithm,TBS).

public_key(?'sha1WithRSAEncryption',TBS) ->
    PublicKey = TBS#'OTPTBSCertificate'.subjectPublicKeyInfo,
    #'RSAPublicKey'{modulus = N,publicExponent = E} = PublicKey#'OTPSubjectPublicKeyInfo'.subjectPublicKey,
    {{rsa,sha},[E,N]};

public_key(?'sha224WithRSAEncryption',TBS) ->
    PublicKey = TBS#'OTPTBSCertificate'.subjectPublicKeyInfo,
    #'RSAPublicKey'{modulus = N,publicExponent = E} = PublicKey#'OTPSubjectPublicKeyInfo'.subjectPublicKey,
    {{rsa,sha224},[E,N]};

public_key(?'sha256WithRSAEncryption',TBS)->
    PublicKey = TBS#'OTPTBSCertificate'.subjectPublicKeyInfo,
    #'RSAPublicKey'{modulus = N,publicExponent = E} = PublicKey#'OTPSubjectPublicKeyInfo'.subjectPublicKey,
    {{rsa,sha256},[E,N]};

public_key(?'sha384WithRSAEncryption',TBS) ->
    PublicKey = TBS#'OTPTBSCertificate'.subjectPublicKeyInfo,
    #'RSAPublicKey'{modulus = N,publicExponent = E} = PublicKey#'OTPSubjectPublicKeyInfo'.subjectPublicKey,
    {{rsa,sha384},[E,N]};

public_key(?'sha512WithRSAEncryption',TBS) ->
    PublicKey = TBS#'OTPTBSCertificate'.subjectPublicKeyInfo,
    #'RSAPublicKey'{modulus = N,publicExponent = E} = PublicKey#'OTPSubjectPublicKeyInfo'.subjectPublicKey,
    {{rsa,sha512},[E,N]}.


decode_otp(Bin)->
	[{'Certificate', Data, not_encrypted}] = public_key:pem_decode(Bin),
	public_key:pkix_decode_cert(Data, otp).
