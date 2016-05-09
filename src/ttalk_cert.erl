-module(ttalk_cert).
-author("David.Alpha.Fox@gmail.com").
-export([cert_public_key/1]).
cert_public_key(X509) ->
	#'OTPCertificate'{tbsCertificate = TBS} = public_key:pkix_decode_cert(X509, otp),
	PublicKey = TBS#'OTPTBSCertificate'.subjectPublicKeyInfo,
	#'RSAPublicKey'{modulus = N,publicExponent = E} = PublicKey#'OTPSubjectPublicKeyInfo'.subjectPublicKey,
	[E,N].
