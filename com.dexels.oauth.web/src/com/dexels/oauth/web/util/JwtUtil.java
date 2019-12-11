package com.dexels.oauth.web.util;

import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.exception.InvalidSWTTokenException;
import com.dexels.oauth.web.commands.OauthCommandOpenID;
import com.fasterxml.jackson.databind.node.POJONode;
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.DirectDecrypter;
import com.nimbusds.jose.crypto.DirectEncrypter;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.EncryptedJWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTClaimsSet.Builder;
import com.nimbusds.jwt.SignedJWT;

@Component(name = "dexels.oauth.jwtutil", service = JwtUtil.class, immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class JwtUtil {
    private static final Logger logger = LoggerFactory.getLogger(JwtUtil.class);

    public static final String OPENID_SUB = "sub";
    public static final String OPENID_NONCE = "nonce";
    public static final String OPENID_NAME = "name";
    public static final String OPENID_GIVENNAME = "given_name";
    public static final String OPENID_FAMILYNAME = "family_name";
    public static final String OPENID_PERSONID = "personid";
    public static final String OPENID_GENDER = "gender";
    public static final String OPENID_EMAIL = "email";
    public static final String OPENID_PICTURE = "picture";
    public static final String OPENID_EMAIL_VERIFIED = "email_verified";

    public static final String OPENID_NICKNAME = "nickname";
    public static final String OPENID_PREF_USERNAME = "preferred_username";

    private static JwtUtil instance;

    private String issuer;
    private String authEndPoint;
    private String tokenEndPoint;
    private String userInfoEndpoint;
    private String jwksUri;

    private OctetSequenceKey ssoKey;
    private RSAKey openidKey;

    @Activate
    public void activate(BundleContext bc, Map<String, Object> settings) {
        String ssokeyString = (String) settings.get("ssokey");
        if (ssokeyString == null || ssokeyString.trim().equals("")) {
            logger.error("Invalid config for SWTUtil");
            throw new IllegalStateException("Invalid config for SWTUtil");
        }

        try {
            ssoKey = OctetSequenceKey.parse(ssokeyString);
        } catch (ParseException e) {
            logger.error("error parsing sso key!");
            throw new RuntimeException(e);
        }

        String openidString = (String) settings.get("openidkey");
        if (openidString == null || openidString.trim().equals("")) {
            logger.error("Invalid config for SWTUtil");
            throw new IllegalStateException("Invalid config for SWTUtil");
        }

        try {
            openidKey = RSAKey.parse(openidString);
        } catch (ParseException e) {
            logger.error("error parsing sso key!");
            throw new RuntimeException(e);
        }

        issuer = (String) settings.get("issuer");
        authEndPoint = (String) settings.get("authEndPoint");
        tokenEndPoint = (String) settings.get("tokenEndPoint");
        userInfoEndpoint = (String) settings.get("userInfoEndpoint");
        jwksUri = (String) settings.get("jwksUri");

        JwtUtil.instance = this;
    }

    public static RSAKey getOpenidKey() {
        if (instance == null) {
            throw new IllegalStateException("Missing JwtUtil instance");
        }
        return instance.openidKey.toPublicJWK();
    }

    public static String generateSSOToken(String subject) {
        if (instance == null) {
            throw new IllegalStateException("Missing JwtUtil instance");
        }

        // We going to create a JWE - encrypted json web token
        JWEObject jweObject = new JWEObject(new JWEHeader(JWEAlgorithm.DIR, EncryptionMethod.A256CBC_HS512), new Payload(subject));

        // Perform encryption
        try {
            jweObject.encrypt(new DirectEncrypter(instance.ssoKey.toByteArray()));
        } catch (JOSEException e) {
            logger.error("Error encrypting!", e);
            return null;
        }

        // Serialise to JWE compact form
        String jweString = jweObject.serialize();
        return jweString;
    }

    public static String getSSOSubject(String token) throws InvalidSWTTokenException {
        if (instance == null) {
            throw new IllegalStateException("Missing JwtUtil instance");
        }

        try {
            try {
                EncryptedJWT jweObject = EncryptedJWT.parse(token);
               
                // Make sure the token has the expected headers
                if ( jweObject.getHeader().getAlgorithm() != JWEAlgorithm.DIR || 
                        jweObject.getHeader().getEncryptionMethod() != EncryptionMethod.A256CBC_HS512) {
                    logger.error("This should not happen - Invalid SSO token! Something fishy is going on here...");
                    throw new InvalidSWTTokenException();
                }
                jweObject.decrypt(new DirectDecrypter(instance.ssoKey.toByteArray()));
                return jweObject.getPayload().toString();
            } catch (java.text.ParseException e) {
                logger.error("Error decrypting!", e);
                throw new InvalidSWTTokenException();
            }

        } catch (Throwable t) {
            throw new InvalidSWTTokenException();
        }
    }

    public static String generateToken(OAuthToken token, OauthUser user) {
        if (instance == null) {
            throw new IllegalStateException("Missing JwtUtil instance");
        }

        String nonce = (String) user.getOpenIDAttributes().get(OPENID_NONCE);
        String sub = (String) user.getOpenIDAttributes().get(OPENID_SUB);

        if (sub == null || "".equals(sub.trim())) {
            logger.warn("Invalid sub {} - this should not happen! Unable to generate openid token", sub);
            return null;
        }

        Date issuesAt = new Date();
        Builder jwtClaimsBuilder = new JWTClaimsSet.Builder()
                .issuer(instance.issuer)
                .subject(sub)
                .audience(token.getClientId())
                .expirationTime(new Date(issuesAt.getTime() + 1000 * 60 * 60)) // expires in 60 minutes
                .notBeforeTime(issuesAt)
                .issueTime(issuesAt)
                .jwtID(UUID.randomUUID().toString());

        if (nonce != null) {
            jwtClaimsBuilder.claim("nonce", nonce);
        }
        addClaims(token, jwtClaimsBuilder);
        
        JWTClaimsSet jwtClaims = jwtClaimsBuilder.build();

        // Create RSA-signer with the private key
        try {
            JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(getOpenidKey().getKeyID()).build();
            
            JWSSigner signer = new RSASSASigner(instance.openidKey.toPrivateKey());
            SignedJWT signedJWT = new SignedJWT(header, jwtClaims);
            signedJWT.sign(signer);
            return signedJWT.serialize();

        } catch (JOSEException e) {
            logger.error("Exception on generating JWT!", e);
            return null;
        }
    }

    private static void addClaims(OAuthToken token, Builder jwtClaimsBuilder) {
        Set<String> claims = new HashSet<>();
        for (Scope s : token.getScopes()) {
            if (s.getId().equals("profile")) {
                claims.addAll(OauthCommandOpenID.TOKEN_PROFILE_CLAIMS);
                claims.add(JwtUtil.OPENID_PERSONID);
            }
            if (s.getId().equals("email")) {
                claims.addAll(OauthCommandOpenID.EMAIL_CLAIMS);
            }
        }
        for (String claim : claims) {
            if (token.getUser().getOpenIDAttributes().containsKey(claim)) {
                jwtClaimsBuilder.claim(claim, token.getUser().getOpenIDAttributes().get(claim));
            }
        }
    }

    public static String getIssuer() {
        return instance.issuer;
    }

    public static String getAuthEndpoint() {
        return instance.issuer + "/" + instance.authEndPoint;
    }

    public static String getTokenEndpoint() {
        return instance.issuer + "/" + instance.tokenEndPoint;
    }

    public static String getUserInfoEndpoint() {
        return instance.issuer + "/" + instance.userInfoEndpoint;
    }

    public static String getJwksUri() {
        return instance.issuer + "/" + instance.jwksUri;
    }

}
